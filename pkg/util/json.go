package util

import (
	"encoding/json"
	"io"
	"io/ioutil"
)

// ReadJSONFromBody returns parsed json value
func ReadJSONFromBody(from io.ReadCloser, value interface{}) error {
	data, err := ioutil.ReadAll(from)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		err = json.Unmarshal(data, value)
		if err != nil {
			return err
		}
	}

	return nil
}
