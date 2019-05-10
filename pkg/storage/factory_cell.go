package storage

import (
	"net/url"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/storage/cell"
)

const (
	paramProxies      = "proxy"
	paramMaxActive    = "maxActive"
	paramMaxIdle      = "maxIdle"
	paramIdleTimeout  = "idleTimeout"
	paramDailTimeout  = "dailTimeout"
	paramReadTimeout  = "readTimeout"
	paramWriteTimeout = "writeTimeout"
)

// example: cell://ip:port?retry=3&maxActive=100&maxIdle=10&idleTimeout=30&dailTimeout=10&readTimeout=30&writeTimeout=10
func createElasticellStorage(u *url.URL) (Storage, error) {
	var cellOpts []cedis.Option

	var proxies []string
	proxies = append(proxies, u.Host)
	if values, ok := u.Query()[paramProxies]; ok {
		proxies = append(proxies, values...)
	}
	cellOpts = append(cellOpts, cedis.WithCellProxies(proxies...))

	maxActive := u.Query().Get(paramMaxActive)
	if maxActive != "" {
		cellOpts = append(cellOpts, cedis.WithMaxActive(format.MustParseStrInt(maxActive)))
	}

	maxIdle := u.Query().Get(paramMaxIdle)
	if maxIdle != "" {
		cellOpts = append(cellOpts, cedis.WithMaxIdle(format.MustParseStrInt(maxIdle)))
	}

	idleTimeout := u.Query().Get(paramIdleTimeout)
	if idleTimeout != "" {
		cellOpts = append(cellOpts, cedis.WithIdleTimeout(time.Second*time.Duration(format.MustParseStrInt64(idleTimeout))))
	}

	dailTimeout := u.Query().Get(paramDailTimeout)
	if idleTimeout != "" {
		cellOpts = append(cellOpts, cedis.WithDailTimeout(time.Second*time.Duration(format.MustParseStrInt64(dailTimeout))))
	}

	readTimeout := u.Query().Get(paramReadTimeout)
	if readTimeout != "" {
		cellOpts = append(cellOpts, cedis.WithReadTimeout(time.Second*time.Duration(format.MustParseStrInt64(readTimeout))))
	}

	writeTimeout := u.Query().Get(paramWriteTimeout)
	if readTimeout != "" {
		cellOpts = append(cellOpts, cedis.WithWriteTimeout(time.Second*time.Duration(format.MustParseStrInt64(writeTimeout))))
	}

	var opts []cell.Option
	retry := u.Query().Get(paramMaxRetryTimes)
	if retry != "" {
		opts = append(opts, cell.WithRetry(format.MustParseStrInt(retry)))
	}

	return cell.NewStorage(cedis.NewCedis(cellOpts...), opts...), nil
}
