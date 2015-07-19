package cli

import (
	"github.com/spf13/cobra"
)

var sunbaseCommand = &cobra.Command{
	name: "sunbase",
	use:  "sunbase",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
	},
}
