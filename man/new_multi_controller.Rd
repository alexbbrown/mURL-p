\name{new_multi_controller}
\alias{new_multi_controller}
\title{Create a new multi url controller for the shiny server}
\usage{
  new_multi_controller(session)
}
\arguments{
  \item{session}{the shiny session}
}
\value{
  a reactive multi controller.
}
\description{
  The multi url controller manages and schedules parallel
  downloads
}
\details{
  Each shiny session should get at least one multi url
  handler.  They should be created per session.
}

