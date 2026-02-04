package names

import (
	"fmt"

	"github.com/gezibash/arc/v2/internal/cli"
	"github.com/gezibash/arc/v2/internal/config"
	internalnames "github.com/gezibash/arc/v2/internal/names"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Entrypoint returns the names command.
func Entrypoint(v *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "names",
		Short: "Manage local name->pubkey mappings",
		Long: `Manage your local name directory for resolving @names to public keys.

When you send to @alice and alice is in your names, the message is
addressed directly to her public key without relay lookup.

Examples:
  arc names list
  arc names add alice 7f3a8b9c...
  arc names remove alice
  arc names lookup alice`,
	}

	cmd.AddCommand(
		listCmd(v),
		addCmd(v),
		removeCmd(v),
		lookupCmd(v),
	)

	return cmd
}

func listCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all name entries",
		RunE: func(cmd *cobra.Command, args []string) error {
			store := internalnames.New(dataDir(v))
			if err := store.Load(); err != nil {
				return err
			}

			entries := store.List()
			out := cli.NewOutputFromViper(v)

			if len(entries) == 0 {
				return out.Result("names-list", "No names configured").Render()
			}

			table := out.Table("names-list", "Name", "Public Key")
			for _, e := range entries {
				table.AddRow("@"+e.Name, e.Pubkey)
			}

			return table.Render()
		},
	}
}

func addCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "add <name> <pubkey>",
		Short: "Add or update a name entry",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, pubkey := args[0], args[1]

			store := internalnames.New(dataDir(v))
			if err := store.Load(); err != nil {
				return err
			}

			if err := store.Add(name, pubkey); err != nil {
				return err
			}

			out := cli.NewOutputFromViper(v)
			return out.Result("name-added", fmt.Sprintf("Added @%s", name)).
				With("Name", "@"+name).
				With("Public Key", pubkey).
				Render()
		},
	}
}

func removeCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a name entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			store := internalnames.New(dataDir(v))
			if err := store.Load(); err != nil {
				return err
			}

			if err := store.Remove(name); err != nil {
				return err
			}

			out := cli.NewOutputFromViper(v)
			return out.Result("name-removed", fmt.Sprintf("Removed @%s", name)).
				With("Name", "@"+name).
				Render()
		},
	}
}

func lookupCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "lookup <name>",
		Short: "Look up a name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			store := internalnames.New(dataDir(v))
			if err := store.Load(); err != nil {
				return err
			}

			pubkey, err := store.Lookup(name)
			if err != nil {
				return fmt.Errorf("@%s not found", name)
			}

			out := cli.NewOutputFromViper(v)
			return out.KV("name-lookup").
				Set("Name", "@"+name).
				Set("Public Key", pubkey).
				Render()
		},
	}
}

func dataDir(v *viper.Viper) string {
	if d := v.GetString("data_dir"); d != "" {
		return d
	}
	return config.Common.DataDir
}
