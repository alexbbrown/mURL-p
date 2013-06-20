#' build an worker for parallel downloads
#'
#' URLworker is concerned with the continuing fetch work of the multi
#' curl handle.  It also tickles completed fetches so they can react.
#' It currently uses a timer, but could use select in a parallel universe.
#' @param murl the multi controller handler
#' @param session the shiny session object
#' @return an observer which automatically downloads pending urls
makeUrlWorker <- function(murl,session)observe({
	
	murl$complete # start trigger
	
	if (murl$complete == TRUE||length(murl$fetchers)==0) return(NULL)
	
	# do one more unit of work.  Can we get it to do an intermediate amount of work?
	# may want to globally adjust buffer sizes
	# let's do at least .1 seconds of work
	startTime <- now()
	while(difftime(now(),startTime,units="secs") < .2) {
		status <- curlMultiPerform(murl$multiHandle, multiple = FALSE)
		if (status$numHandlesRemaining == 0) break
  }

  # scan for completed handles.  Ideally should rely on curl multi info api
  # but it's not currently exposed through rcurl.  instead check that download size
  # equals content size
	if (status$numHandlesRemaining != isolate(murl$lastHandlesRemaining) ||
	  murl$startedCount != murl$lastStartedCount) {
			
			
		murl$fetchers <- Filter(function(fetcher){
			status <- simpleStatus(isolate(fetcher$deferred_httr$curl))
			completeSize <- status$content.length.download
			if (!is.null(fetcher$size)) completeSize<-fetcher$size
			complete <- completeSize==status$size.download | status$response.code >= 400

			if (complete) {
				if (status$response.code >= 400) {
					cat(file=stderr(),"failed to download url: ", status$effective.url, " ", fetcher$opts$url, "\n")
					fetcher$content <- NULL
				} else {
        	cat(file=stderr(),"completed download of url: ", status$effective.url, " ", fetcher$opts$url, "\n")
					# decode content when complete.  this triggers the next step (consumer)
	        # note - content may already be decoded by the response function - let's check
	        # embedded nulls when decoding can be a problem.
					response <- fetcher$deferred_httr$response()
					if (is(response, "response")) {
						fetcher$content <- content(as="text",response)
					} else {
						fetcher$content <- response 
					}
				}
				
				pop(murl$multiHandle, isolate(fetcher$deferred_httr$curl))
			
				murl$completed <<- c(murl$completed, list(fetcher))
			}
			return(!all(complete))
		},murl$fetchers)
	}
	
	murl$lastHandlesRemaining <<- status$numHandlesRemaining
	murl$lastStartedCount <<- isolate(murl$startedCount)
	
	if (status$numHandlesRemaining > 0) {
		invalidateLater(10,session)
		# libcurl knows when curlhandles are complete, but that property is not exported
		# this code uses the contentlength to detect completeness
	} else {
		# all tasks are complete.  Trigger completeness tag
		# should be more selective - trigger each as they complete.			
		murl$complete <<- TRUE
	}
	# progressCount is used to inform any progress monitors
	murl$progressCount <<- isolate(murl$progressCount)+1
})

#' Create a new multi url controller for the shiny server
#' 
#' The multi url controller manages and schedules parallel downloads 
#' 
#' Each shiny session should get at least one multi url handler.  They should be
#' created per session.
#' 
#' @export
#' @param session the shiny session
#' @return a reactive multi controller.
new_multi_controller <- function(session){
	murl <- reactiveValues(
		multiHandle = getCurlMultiHandle(),
		complete = FALSE, # should probably be 2 to start
		progressCount = 0, # just indicates progress
		startedCount = 0, # used to help decide to check completeness
		fetchers = list(),
		completed = list(),
		downloads = list(),
		lastStartedCount = 0, # used to help decide to check completeness
		lastHandlesRemaining = 0 # used to help decide to check completeness
	)
	murl$urlWorker <- makeUrlWorker(murl,session)
	murl
}

#' Tell the multi controller to start a new url for download
#' @param url the url to download
#' @param murl the multi url controller
#' @return a wrapper around a deferred http handle.
#'
#' more documentation required.
#'
#' @export
queue_download <- function(url, murl) {
	deferred_httr <- GET_deferred(url)
	queue_deferred(deferred_httr,url,murl)
}

#' Tell the multi controller to start a new url for download
#' @param deferred_httr the deferred httr request
#' @param url that is requested or a descriptive name
#' @param murl the multi url controller
#' @return a wrapper around a deferred http handle.
#'
#' more documentation required.
#'
#' @export
queue_deferred <- function(deferred_httr, url, murl) {
	new_fetcher <- new.fetcher(url)
	new_fetcher$deferred_httr <- deferred_httr

	push(murl$multiHandle, deferred_httr$curl)

	murl$fetchers <- c(isolate(murl$fetchers),list(new_fetcher))
	
	murl$downloads <- c(list(new_fetcher),isolate(murl$downloads))
	
	cat(file=stderr(),"count",length(isolate(murl$fetchers)),"\n")
	
	murl$complete <- FALSE

	# progressCount is used to inform any progress monitors and the urlWorker
	murl$progressCount <- isolate(murl$progressCount) + 1
  # startedCount can be used to update reactives
	murl$startedCount <- isolate(murl$startedCount) + 1
	
	new_fetcher
}

#' Create a new easy handle with data to allow it to signal completion
#' and extract content
#'
#' @param url the url to download
#' @return a new fetcher
new.fetcher <- function(url) {
	reactiveValues(
		url = url,             # the original URL being downloaded
		uid = tempfile("",""), # unique ID for disambiguating and for naming outputs
    deferred_httr = NULL,  # the 'handle' for this asynchronous get.  contains status.
    complete = FALSE,			 # has the download finished
		content = NULL         # content.  set when complete, NULL otherwise
	)
}

nullNa <- function(x)lapply(x,function(x){
  if (length(x)==0) return(NA)
  if (length(x)>1) return(paste(x,collapse=", "))
  if (is.null(x)) return(NA)
  x
})

#' return the status of a fetcher
#'
#' @export
#' @param handle a fetcher
simpleStatus <- function(handle) {
	ldply(list(handle),function(x)as.data.frame(nullNa(getCurlInfo(x))))
}
