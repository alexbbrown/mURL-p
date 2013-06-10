\name{makeUrlWorker}
\alias{makeUrlWorker}
\title{build an worker for parallel downloads}
\usage{
  makeUrlWorker(murl, session)
}
\arguments{
  \item{murl}{the multi controller handler}

  \item{session}{the shiny session object}
}
\value{
  an observer which automatically downloads pending urls
}
\description{
  URLworker is concerned with the continuing fetch work of
  the multi curl handle.  It also tickles completed fetches
  so they can react. It currently uses a timer, but could
  use select in a parallel universe.
}

