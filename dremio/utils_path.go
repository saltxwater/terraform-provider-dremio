package dremio

import (
	"log"
	"strings"

	dapi "github.com/saltxwater/go-dremio-api-client"
)

func getAbsolutePath(client *dapi.Client, parentId string, relativePath []interface{}) ([]string, error) {
	log.Printf("Using catalog entry with id '%s' as root", parentId)
	parent, err := client.GetCatalogEntityById(parentId)
	if err != nil {
		return nil, err
	}
	relPath := interfaceListToStringList(relativePath)
	return append(parent.Path, relPath...), nil
}

func getQueryPath(path []string) string {
	qp := make([]string, len(path))
	for i, p := range path {
		qp[i] = "\"" + p + "\""
	}
	return strings.Join(qp, ".")
}
