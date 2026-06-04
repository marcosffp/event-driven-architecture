package worker

import (
	"fmt"
	"os"
)

func simulatedFailure(processor string) error {
	if os.Getenv("FAIL_RATE") != "1" {
		return nil
	}
	return fmt.Errorf("%s: falha simulada para demo (FAIL_RATE=1)", processor)
}
