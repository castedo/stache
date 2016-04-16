\name{LastUTCWeekday}
\alias{LastUTCWeekday}
\title{LastUTCWeekday}
\description{Returns the last completed weekday in UTC (GMT) timezone.}
\value{Most recent date relative to UTC time}
\arguments{Optionally you can pass in a date as if it currently was that date.}
\examples{
  LastUTCWeekday()
  LastUTCWeekday("2001-01-01") # returns 2000-12-29
}

