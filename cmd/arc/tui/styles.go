package tui

import "github.com/charmbracelet/lipgloss"

// Shared colors.
var (
	AccentColor = lipgloss.AdaptiveColor{Light: "#874BFD", Dark: "#7D56F4"}
	DimColor    = lipgloss.AdaptiveColor{Light: "#A49FA5", Dark: "#777777"}
	WarnColor   = lipgloss.AdaptiveColor{Light: "#F25D94", Dark: "#F25D94"}
	GreenColor  = lipgloss.AdaptiveColor{Light: "#43BF6D", Dark: "#73F59F"}
)

// Shared styles.
var (
	TitleStyle = lipgloss.NewStyle().
			Foreground(AccentColor).
			Bold(true)

	SubtitleStyle = lipgloss.NewStyle().
			Foreground(DimColor)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(WarnColor).
			Bold(true)

	HelpStyle = lipgloss.NewStyle().
			Foreground(DimColor)

	RefStyle = lipgloss.NewStyle().
			Foreground(AccentColor).
			Bold(true)

	AgeStyle = lipgloss.NewStyle().
			Foreground(DimColor)

	PreviewStyle = lipgloss.NewStyle().
			Foreground(DimColor)

	CursorStyle = lipgloss.NewStyle().
			Foreground(AccentColor).
			Bold(true)

	PeerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.AdaptiveColor{Light: "#D4A017", Dark: "#FFD866"}).
			Bold(true)
)
