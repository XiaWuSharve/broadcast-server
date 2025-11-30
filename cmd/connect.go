/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/gorilla/websocket"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

// connectCmd represents the connect command
var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Connect to a broadcast server.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		client := &websocket.Dialer{}
		conn, _, err := client.Dial("ws://localhost:8080/ws", http.Header{})
		if err != nil {
			fmt.Printf("client: connection failed: %v\n", err)
		}

		textArea := tview.NewTextArea().SetPlaceholder("Alt+Enter to send messages")
		view := tview.NewTextView().SetScrollable(true)

		grid := tview.NewGrid().
			SetRows(0, 1).
			SetColumns(0).
			SetBorders(true).
			AddItem(view, 0, 0, 1, 1, 0, 0, false).
			AddItem(textArea, 1, 0, 1, 1, 0, 0, true)
		app := tview.NewApplication().SetRoot(grid, true).SetFocus(grid)

		textArea.SetChangedFunc(func() {
			grid.SetRows(0, strings.Count(textArea.GetText(), "\n")+1).
				SetDrawFunc(func(screen tcell.Screen, x, y, width, height int) (int, int, int, int) {
					textArea.SetOffset(0, 0)
					return x, y, width, height
				})
		})

		textArea.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
			if event.Modifiers() == tcell.ModAlt && event.Key() == tcell.KeyEnter {
				txt := textArea.GetText()
				txt = strings.Trim(txt, " \n")
				if txt == "" {
					return nil
				}
				conn.WriteMessage(websocket.TextMessage, []byte(txt))
				fmt.Fprintf(view, "%s\n", txt)
				textArea.SetText("", true)
				return nil
			}
			return event
		})

		go func() {
			syncWrite := func(data any) {
				app.QueueUpdateDraw(func() {
					fmt.Fprintf(view, "%s\n", data)
				})
			}
			for {
				_, data, err := conn.ReadMessage()
				if err != nil {
					syncWrite(fmt.Sprintf("read: %v\n", err))
					break
				}
				syncWrite(data)
			}
		}()

		view.SetChangedFunc(func() {
			view.ScrollToEnd()
		})

		if err := app.Run(); err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(connectCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// connectCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// connectCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
