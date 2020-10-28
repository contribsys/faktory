// Generated by ego.
// DO NOT EDIT

//line morgue.ego:1

package webui

import "fmt"
import "html"
import "io"
import "context"

import (
	"net/http"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
)

func ego_listDead(w io.Writer, req *http.Request, set storage.SortedSet, count, currentPage uint64) {
	totalSize := uint64(set.Size())

//line morgue.ego:14
	_, _ = io.WriteString(w, "\n\n")
//line morgue.ego:15
	ego_layout(w, req, func() {
//line morgue.ego:16
		_, _ = io.WriteString(w, "\n\n<header class=\"row\">\n  <div class=\"col-sm-5\">\n    <h3>")
//line morgue.ego:19
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "DeadJobs"))))
//line morgue.ego:19
		_, _ = io.WriteString(w, "</h3>\n  </div>\n  ")
//line morgue.ego:21
		if totalSize > count {
//line morgue.ego:22
			_, _ = io.WriteString(w, "\n    <div class=\"col-sm-4\">\n      ")
//line morgue.ego:23
			ego_paging(w, req, "/morgue", totalSize, count, currentPage)
//line morgue.ego:24
			_, _ = io.WriteString(w, "\n    </div>\n  ")
//line morgue.ego:25
		}
//line morgue.ego:26
		_, _ = io.WriteString(w, "\n  ")
//line morgue.ego:26
		_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(filtering("dead"))))
//line morgue.ego:27
		_, _ = io.WriteString(w, "\n</header>\n\n")
//line morgue.ego:29
		if totalSize > uint64(0) {
//line morgue.ego:30
			_, _ = io.WriteString(w, "\n  <form action=\"")
//line morgue.ego:30
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(root(req))))
//line morgue.ego:30
			_, _ = io.WriteString(w, "/morgue\" method=\"post\">\n    ")
//line morgue.ego:31
			_, _ = fmt.Fprint(w, csrfTag(req))
//line morgue.ego:32
			_, _ = io.WriteString(w, "\n    <div class=\"table_container\">\n      <table class=\"table table-striped table-bordered table-white\">\n        <thead>\n          <tr>\n            <th class=\"table-checkbox checkbox-column\">\n              <label>\n                <input type=\"checkbox\" class=\"check_all\" />\n              </label>\n            </th>\n            <th>")
//line morgue.ego:41
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "LastRetry"))))
//line morgue.ego:41
			_, _ = io.WriteString(w, "</th>\n            <th>")
//line morgue.ego:42
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Queue"))))
//line morgue.ego:42
			_, _ = io.WriteString(w, "</th>\n            <th>")
//line morgue.ego:43
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Job"))))
//line morgue.ego:43
			_, _ = io.WriteString(w, "</th>\n            <th>")
//line morgue.ego:44
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Arguments"))))
//line morgue.ego:44
			_, _ = io.WriteString(w, "</th>\n            <th>")
//line morgue.ego:45
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Error"))))
//line morgue.ego:45
			_, _ = io.WriteString(w, "</th>\n          </tr>\n        </thead>\n        ")
//line morgue.ego:48
			setJobs(set, count, currentPage, func(idx int, key []byte, job *client.Job) {
//line morgue.ego:49
				_, _ = io.WriteString(w, "\n          <tr>\n            <td class=\"table-checkbox\">\n              <label>\n                <input type=\"checkbox\" name=\"key\" value=\"")
//line morgue.ego:52
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(string(key))))
//line morgue.ego:52
				_, _ = io.WriteString(w, "\" />\n              </label>\n            </td>\n            <td>\n              <a href=\"")
//line morgue.ego:56
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(root(req))))
//line morgue.ego:56
				_, _ = io.WriteString(w, "/morgue/")
//line morgue.ego:56
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(string(key))))
//line morgue.ego:56
				_, _ = io.WriteString(w, "\">")
//line morgue.ego:56
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(relativeTime(job.EnqueuedAt))))
//line morgue.ego:56
				_, _ = io.WriteString(w, "</a>\n            </td>\n            <td>\n              <a href=\"")
//line morgue.ego:59
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(root(req))))
//line morgue.ego:59
				_, _ = io.WriteString(w, "/queues/")
//line morgue.ego:59
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Queue)))
//line morgue.ego:59
				_, _ = io.WriteString(w, "\">")
//line morgue.ego:59
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Queue)))
//line morgue.ego:59
				_, _ = io.WriteString(w, "</a>\n            </td>\n            <td><code>")
//line morgue.ego:61
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Type)))
//line morgue.ego:61
				_, _ = io.WriteString(w, "</code></td>\n            <td>\n              <div class=\"args\">")
//line morgue.ego:63
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(displayArgs(job.Args))))
//line morgue.ego:63
				_, _ = io.WriteString(w, "</div>\n            </td>\n            <td>\n              ")
//line morgue.ego:66
				if job.Failure != nil {
//line morgue.ego:67
					_, _ = io.WriteString(w, "\n              <div>")
//line morgue.ego:67
					_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Failure.ErrorType)))
//line morgue.ego:67
					_, _ = io.WriteString(w, ": ")
//line morgue.ego:67
					_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(job.Failure.ErrorMessage)))
//line morgue.ego:67
					_, _ = io.WriteString(w, "</div>\n              ")
//line morgue.ego:68
				}
//line morgue.ego:69
				_, _ = io.WriteString(w, "\n            </td>\n          </tr>\n        ")
//line morgue.ego:71
			})
//line morgue.ego:72
			_, _ = io.WriteString(w, "\n      </table>\n    </div>\n    <div class=\"pull-left flip\">\n      <button class=\"btn btn-primary btn-sm\" type=\"submit\" name=\"action\" value=\"retry\">")
//line morgue.ego:75
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "RetryNow"))))
//line morgue.ego:75
			_, _ = io.WriteString(w, "</button>\n      <button class=\"btn btn-danger btn-sm\" type=\"submit\" name=\"action\" value=\"delete\">")
//line morgue.ego:76
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "Delete"))))
//line morgue.ego:76
			_, _ = io.WriteString(w, "</button>\n    </div>\n  </form>\n\n  ")
//line morgue.ego:80
			if unfiltered() {
//line morgue.ego:81
				_, _ = io.WriteString(w, "\n    <form action=\"")
//line morgue.ego:81
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(root(req))))
//line morgue.ego:81
				_, _ = io.WriteString(w, "/morgue\" method=\"post\">\n      ")
//line morgue.ego:82
				_, _ = fmt.Fprint(w, csrfTag(req))
//line morgue.ego:83
				_, _ = io.WriteString(w, "\n      <input type=\"hidden\" name=\"key\" value=\"all\" />\n      <div class=\"pull-right flip\">\n        <button class=\"btn btn-primary btn-sm\" type=\"submit\" name=\"action\" value=\"retry\">")
//line morgue.ego:85
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "RetryAll"))))
//line morgue.ego:85
				_, _ = io.WriteString(w, "</button>\n        <button class=\"btn btn-danger btn-sm\" type=\"submit\" name=\"action\" value=\"delete\">")
//line morgue.ego:86
				_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "DeleteAll"))))
//line morgue.ego:86
				_, _ = io.WriteString(w, "</button>\n      </div>\n    </form>\n  ")
//line morgue.ego:89
			}
//line morgue.ego:90
			_, _ = io.WriteString(w, "\n\n")
//line morgue.ego:91
		} else {
//line morgue.ego:92
			_, _ = io.WriteString(w, "\n  <div class=\"alert alert-success\">")
//line morgue.ego:92
			_, _ = io.WriteString(w, html.EscapeString(fmt.Sprint(t(req, "NoDeadJobsFound"))))
//line morgue.ego:92
			_, _ = io.WriteString(w, "</div>\n")
//line morgue.ego:93
		}
//line morgue.ego:94
		_, _ = io.WriteString(w, "\n")
//line morgue.ego:94
	})
//line morgue.ego:95
	_, _ = io.WriteString(w, "\n")
//line morgue.ego:95
}

var _ fmt.Stringer
var _ io.Reader
var _ context.Context
var _ = html.EscapeString
