package addressbook

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/gezibash/arc-node/internal/addressbook"
	"github.com/gezibash/arc-node/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Entrypoint returns the addressbook command.
func Entrypoint(v *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "addressbook",
		Short: "Manage local addressbook (name to pubkey mappings)",
		Long: `Manage your local addressbook for resolving @names to public keys.

The addressbook provides local resolution before falling back to the relay.
When you send to @alice and alice is in your addressbook, the message is
addressed directly to her public key.

Examples:
  arc addressbook list
  arc addressbook add alice 7f3a8b9c...
  arc addressbook remove alice
  arc addressbook lookup alice`,
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
		Short: "List all addressbook entries",
		RunE: func(cmd *cobra.Command, args []string) error {
			ab := addressbook.New(dataDir(v))
			if err := ab.Load(); err != nil {
				return err
			}

			entries := ab.List()
			if len(entries) == 0 {
				fmt.Println("Addressbook is empty")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "NAME\tPUBKEY")
			for _, e := range entries {
				_, _ = fmt.Fprintf(w, "@%s\t%s\n", e.Name, e.Pubkey)
			}
			return w.Flush()
		},
	}
}

func addCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "add <name> <pubkey>",
		Short: "Add or update an addressbook entry",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, pubkey := args[0], args[1]

			ab := addressbook.New(dataDir(v))
			if err := ab.Load(); err != nil {
				return err
			}

			if err := ab.Add(name, pubkey); err != nil {
				return err
			}

			fmt.Printf("Added @%s → %s\n", name, pubkey)
			return nil
		},
	}
}

func removeCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove an addressbook entry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			ab := addressbook.New(dataDir(v))
			if err := ab.Load(); err != nil {
				return err
			}

			if err := ab.Remove(name); err != nil {
				return err
			}

			fmt.Printf("Removed @%s\n", name)
			return nil
		},
	}
}

func lookupCmd(v *viper.Viper) *cobra.Command {
	return &cobra.Command{
		Use:   "lookup <name>",
		Short: "Look up a name in the addressbook",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			ab := addressbook.New(dataDir(v))
			if err := ab.Load(); err != nil {
				return err
			}

			pubkey, err := ab.Lookup(name)
			if err != nil {
				return fmt.Errorf("@%s not found in addressbook", name)
			}

			fmt.Println(pubkey)
			return nil
		},
	}
}

func dataDir(v *viper.Viper) string {
	if d := v.GetString("data_dir"); d != "" {
		return d
	}
	return config.DefaultDataDir()
}
