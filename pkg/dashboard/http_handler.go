package dashboard

import (
	"errors"
	"net/http"

	"github.com/fagongzi/util/format"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/labstack/echo"
)

const (
	succeed = 0
	failed  = 1
)

const (
	defaultLimit = 50
)

var (
	errMissingParam = errors.New("missing param")
)

func readUInt64Param(name string, ctx echo.Context) (uint64, error) {
	param := ctx.Param(name)
	if param == "" {
		return 0, errMissingParam
	}

	value, err := format.ParseStrUInt64(param)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (s *Dashboard) fragments() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		s.RLock()
		defer s.RUnlock()

		return ctx.JSON(http.StatusOK, &meta.JSONResult{
			Value: s.frags,
		})
	}
}

func (s *Dashboard) transactions() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		fid, err := readUInt64Param("fid", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		query := meta.Query{
			Limit:  defaultLimit,
			After:  0,
			Name:   ctx.QueryParam("name"),
			Status: -1,
			Action: -1,
		}

		limit := ctx.QueryParam("limit")
		if limit != "" {
			value, err := format.ParseStrUInt64(limit)
			if err != nil {
				return ctx.NoContent(http.StatusBadRequest)
			}
			query.Limit = value
		}

		after := ctx.QueryParam("after")
		if after != "" {
			value, err := format.ParseStrUInt64(after)
			if err != nil {
				return ctx.NoContent(http.StatusBadRequest)
			}
			query.After = value
		}

		status := ctx.QueryParam("status")
		if status != "" {
			value, err := format.ParseStrInt(status)
			if err != nil {
				return ctx.NoContent(http.StatusBadRequest)
			}
			query.Status = value
		}

		action := ctx.QueryParam("action")
		if action != "" {
			value, err := format.ParseStrInt(action)
			if err != nil {
				return ctx.NoContent(http.StatusBadRequest)
			}
			query.Action = value
		}

		result := meta.JSONResult{}
		value, err := s.api.Transactions(fid, query)
		result.Value = value
		if err != nil {
			result.Code = failed
			result.Value = err.Error
		}

		return ctx.JSON(http.StatusOK, result)
	}
}

func (s *Dashboard) transaction() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		fid, err := readUInt64Param("fid", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		id, err := readUInt64Param("id", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		result := meta.JSONResult{}
		value, err := s.api.Transaction(fid, id)
		result.Value = value
		if err != nil {
			result.Code = failed
			result.Value = err.Error
		}

		return ctx.JSON(http.StatusOK, result)
	}
}

func (s *Dashboard) commit() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		fid, err := readUInt64Param("fid", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		id, err := readUInt64Param("id", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		result := meta.JSONResult{}
		err = s.manual.Commit(fid, id)
		if err != nil {
			result.Code = failed
			result.Value = err.Error
		}

		return ctx.JSON(http.StatusOK, result)
	}
}

func (s *Dashboard) rollback() func(ctx echo.Context) error {
	return func(ctx echo.Context) error {
		fid, err := readUInt64Param("fid", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		id, err := readUInt64Param("id", ctx)
		if err != nil {
			return ctx.NoContent(http.StatusBadRequest)
		}

		result := meta.JSONResult{}
		err = s.manual.Rollback(fid, id)
		if err != nil {
			result.Code = failed
			result.Value = err.Error
		}

		return ctx.JSON(http.StatusOK, result)
	}
}
