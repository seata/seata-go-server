package prophet

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
)

const (
	// privateFileMode grants owner to read/write a file.
	privateFileMode = 0600
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

func prepareJoin(cfg *EmbeddedEtcdCfg) error {
	if cfg.Join == "" {
		return nil
	}

	// join self not allowed
	if cfg.Join == cfg.URLsAdvertiseClient {
		return fmt.Errorf("can not join self")
	}

	filePath := path.Join(cfg.DataPath, "join")

	// Read the persist join config
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		s, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal("read the join config meet failed with %+v", err)
		}
		cfg.InitialCluster = strings.TrimSpace(string(s))
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	initialCluster := ""

	// already is a member of the cluster
	if isDataExist(path.Join(cfg.DataPath, "member")) {
		cfg.InitialCluster = initialCluster
		cfg.InitialClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Join, ","),
		DialTimeout: DefaultTimeout,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	listResp, err := listEtcdMembers(client)
	if err != nil {
		return err
	}

	existed := false
	for _, m := range listResp.Members {
		if len(m.Name) == 0 {
			return errors.New("there is a member that has not joined successfully")
		}
		if m.Name == cfg.Name {
			existed = true
		}
	}
	// maybe missing data or re-join the cluster
	if existed {
		return errors.New("missing data or join a duplicated cluster")
	}

	// joins an existing cluster.
	addResp, err := addEtcdMember(client, strings.Split(getStringValue(cfg.URLsAdvertisePeer, cfg.URLsPeer), ","))
	if err != nil {
		return err
	}

	listResp, err = listEtcdMembers(client)
	if err != nil {
		return err
	}

	prophets := []string{}
	for _, memb := range listResp.Members {
		n := memb.Name
		if memb.ID == addResp.Member.ID {
			n = cfg.Name
		}
		if len(n) == 0 {
			return errors.New("there is a member that has not joined successfully")
		}
		for _, m := range memb.PeerURLs {
			prophets = append(prophets, fmt.Sprintf("%s=%s", n, m))
		}
	}
	initialCluster = strings.Join(prophets, ",")
	cfg.InitialCluster = initialCluster
	cfg.InitialClusterState = embed.ClusterStateFlagExisting
	err = os.MkdirAll(cfg.DataPath, privateDirMode)
	if err != nil && !os.IsExist(err) {
		return err
	}

	return ioutil.WriteFile(filePath, []byte(cfg.InitialCluster), privateFileMode)
}

func addEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	addResp, err := client.MemberAdd(ctx, urls)
	cancel()
	return addResp, err
}

func listEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	listResp, err := client.MemberList(ctx)
	cancel()
	return listResp, err
}

func isDataExist(d string) bool {
	dir, err := os.Open(d)
	if err != nil {
		log.Error("open directory %s failed with %+v", d, err)
		return false
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		log.Error("list directory %s failed with %+v", d, err)
		return false
	}
	return len(names) != 0
}
