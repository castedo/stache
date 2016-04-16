\name{stache.read}
\alias{stache.read}
\title{stache.read}
\description{Read time series data.}
\arguments{
  \item{tsids}{ vector of path-like time series id }
  \item{cache.id}{ id of cache to use }
}
\examples{
  stache.read(c("/EQ/Q/USD/MSFT/CLOSE", "/EQ/Q/USD/SPY/CLOSE"), "mycache")
}

