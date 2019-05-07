package dashboard

import (
	"context"
	"sync"

	"github.com/fagongzi/util/json"
	"github.com/infinivision/prophet"
	"github.com/infinivision/taas/pkg/core"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/labstack/echo"
)

const (
	version = "/v1"
)

// Dashboard an api dashboard server
type Dashboard struct {
	sync.RWMutex

	cfg    Cfg
	server *echo.Echo
	api    core.QueryAPI
	manual core.ManualAPI

	watcher *prophet.Watcher
	frags   []meta.Fragment
}

// NewDashboard returns a dashboard server
func NewDashboard(cfg Cfg, api core.QueryAPI, manual core.ManualAPI) *Dashboard {
	s := &Dashboard{
		cfg:     cfg,
		server:  echo.New(),
		api:     api,
		manual:  manual,
		watcher: prophet.NewWatcher(cfg.ProphetAddrs...),
	}

	go s.startWatch()
	s.initRoute()
	return s
}

func (s *Dashboard) startWatch() {
	go func() {
		c := s.watcher.Watch(prophet.EventResourceCreated | prophet.EventInit)
		for {
			evt, ok := <-c
			if !ok {
				return
			}

			switch evt.Event {
			case prophet.EventInit:
				s.updateAll(evt)
			case prophet.EventResourceCreated:
				s.addFrag(evt)
			}
		}
	}()
}

func (s *Dashboard) addFrag(evt *prophet.EventNotify) {
	s.Lock()
	defer s.Unlock()

	value := meta.Fragment{}
	json.MustUnmarshal(&value, evt.Value)
	s.frags = append(s.frags, value)
}

func (s *Dashboard) updateAll(evt *prophet.EventNotify) {
	s.Lock()
	defer s.Unlock()

	s.frags = make([]meta.Fragment, 0)

	doFragFunc := func(data []byte, leader uint64) {
		value := meta.Fragment{}
		json.MustUnmarshal(&value, data)
		s.frags = append(s.frags, value)
	}

	evt.ReadInitEventValues(doFragFunc, func(data []byte) {})
}

func (s *Dashboard) initRoute() {
	s.server.Static(s.cfg.UIPrefix, s.cfg.UI)
	versionGroup := s.server.Group(version)
	versionGroup.GET("/fragments", s.fragments())
	versionGroup.GET("/fragments/:fid/transactions", s.transactions())
	versionGroup.GET("/fragments/:fid/transactions/:id", s.transaction())
	versionGroup.PUT("/fragments/:fid/transactions/:id/commit", s.commit())
	versionGroup.PUT("/fragments/:fid/transactions/:id/rollback", s.rollback())
}

// Start start the dashboard
func (s *Dashboard) Start() error {
	return s.server.Start(s.cfg.Addr)
}

// Stop stop the dashboard
func (s *Dashboard) Stop() error {
	return s.server.Shutdown(context.TODO())
}
