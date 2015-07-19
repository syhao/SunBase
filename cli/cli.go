package cli

import (
	"github.com/spf13/cobra"
	"runtime"
	"strings"
	"text/tabwriter"
)

var (
	// These variables are initialized via the linker -X flag in the
	// top-level Makefile when compiling release binaries.
	buildVers string // Go Version
	buildTag  string // Tag of this build (git describe)
	buildTime string // Build time in UTC (year/month/day hour:min:sec)
	buildDeps string // Git SHAs of dependencies
)

// BuildInfo ...
type BuildInfo struct {
	Vers string `json:"goVersion"`
	Tag  string `json:"tag"`
	Time string `json:"time"`
	Deps string `json:"dependencies"`
}

// GetBuildInfo ...
func GetBuildInfo() BuildInfo {
	return BuildInfo{
		Vers: runtime.Version(),
		Tag:  buildTag,
		Time: buildTime,
		Deps: buildDeps,
	}
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Run: func(cmd *cobra.Command, args []string) {
		info := GetBuildInfo()
		w := &tabwriter.Writer{}
		w.Init(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(w, "Build Vers:  %s\n", info.Vers)
		fmt.Fprintf(w, "Build Tag:   %s\n", info.Tag)
		fmt.Fprintf(w, "Build Time:  %s\n", info.Time)
		fmt.Fprintf(w, "Build Deps:\n\t%s\n",
			strings.Replace(strings.Replace(info.Deps, " ", "\n\t", -1), ":", "\t", -1))
		if err := w.Flush(); err != nil {
			log.Fatal(err)
		}
	},
}

var sunbaseCommand = &cobra.Command{
	name: "sunbase",
	use:  "sunbase",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	sunbaseCommand.AddCommand(
		initCmd,
		startCmd,
	)
}
