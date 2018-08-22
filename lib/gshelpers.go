package pitreos

import (
	"fmt"
	"path"
	"strings"
)

func isGSURL(query string) bool {
	if strings.HasPrefix(query, "gs://") {
		return true
	}
	return false
}

func splitGSURL(query string) (bucketName string, backupPath string, err error) {
	if !isGSURL(query) {
		err = fmt.Errorf("Invalid Google Storage URL")
		return
	}
	splitParts := strings.SplitN(query[5:], "/", 2)
	if len(splitParts) == 2 {
		return splitParts[0], splitParts[1], nil
	}
	if len(splitParts) == 1 {
		return splitParts[0], "", nil
	}
	return "", "", fmt.Errorf("Invalid Google Storage URL")
}

func getGSURL(bucketName, filePath string) string {
	fullLocation := path.Join(bucketName, filePath)
	return fmt.Sprintf("gs://%s", fullLocation)

}
