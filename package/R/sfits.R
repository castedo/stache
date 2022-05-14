
dataEnv <- new.env()

set.alphavantage.key <- function(key) {
  assign("alphavantage.key", key, envir=dataEnv)
}

LastUTCWeekday <- function( d=Sys.time() ) {  #as.Date(Sys.time()) is UTC
  lastUTC <- as.Date(d) - 1
  mon_offset <- ( as.numeric(strftime(lastUTC, "%w")) - 1 ) %% 7
  return( lastUTC - max(0, mon_offset - 4) )
}

PENDING_ZOO <- zoo(numeric(), as.Date(character()))
attr(PENDING_ZOO, "pending") <- TRUE

is.pending <- function(z) {
  isTRUE(attr(z, "pending"))
}


#######################################################
## Web Data

WebTempFile <- function(url, ...) {
  ret <- tempfile()
  message("Getting ", url, " ...")
  if ( 0 != tryCatch( download.file(url, ret, ...),
                      error=function(e){1} ) ) {
    unlink(ret)
    ret <- NULL
  }
  return(ret)
}

GetWebCsvZoo <- function( url, index.format="%F",
                          column.name=NULL, header.regex="^Date,") {
  ret <- NULL
  tf <- WebTempFile(url)
  on.exit(unlink(tf))
  if (!is.null(tf)) {
    txt <- readLines(tf)
    headeri = grep(header.regex, txt, ignore.case=TRUE)
    if (length(headeri) > 1) {
      warning(url, " has multiple lines matching '", header.regex, "'")
      headeri <- headeri[1]
    }
    blanki <- which( nchar(txt) == 0 )
    blanki <- blanki[ blanki > headeri ]
    lasti <- if (length(blanki) > 0) blanki[1] - 1 else length(txt)
  
    tc <- textConnection(txt[headeri:lasti])
    ret <- read.zoo( tc, format=index.format, sep=',', header=TRUE, drop=FALSE)
    close(tc)
    if (!is.null(column.name)) {
      names(ret) <- toupper(names(ret))
      ret <- ret[,toupper(column.name)]
    }
  }
  return(ret)
}

#####################################
## Time Series Caching

stache.dir <- function( cache.id ) {
  paste(sep='/', path.expand("~/stache/data"), cache.id)
}

SfitsCachePath <- function( sub.cache.name, cache.id ) {
  paste(sep='/', path.expand("~/stache/data"), cache.id, sub.cache.name)
}

SfitsFilePath <- function( symbol, field, cache.dir ) {
  paste(sep='', cache.dir, '/', tolower(field), '/', toupper(symbol), '.csv')
}

SfitsCacheFileExists <- function( source, symbol, field, cache.id ) {
  cache.dir <- SfitsCachePath(tolower(source), cache.id)
  file.exists(SfitsFilePath(symbol, field, cache.dir))
}

SfitsWriteCache <- function( sfits, symbol, field, cache.dir ) {
  stopifnot( is(sfits, "zoo") )
  stopifnot( is(time(sfits), "Date") )
  stopifnot( is(symbol, "character") )
  stopifnot( length(symbol) == 1 )
  stopifnot( is(field, "character") )
  stopifnot( length(field) == 1 )
  stopifnot( is(cache.dir, "character") )
  stopifnot( length(cache.dir) == 1 )

  path <- SfitsFilePath(symbol, field, cache.dir)
  if (!file.exists(path)) {
    dir.create(dirname(path), recursive=TRUE, showWarnings=FALSE)
  }

  if (is.null(dim(sfits))) {
    sfits <- zoo( as.matrix(sfits), time(sfits) )
    colnames(sfits) <- ""
  }
  write.zoo(sfits, path, index.name="Date", col.names=TRUE, sep=',')
}

SfitsReadCache <- function(path) {
  ret <- NULL
  if (file.exists(path)) {
    ret <- read.zoo( path, "%F", drop=FALSE,
                     header=TRUE, sep=',', check.names=FALSE )
    if (nrow(ret) > 0) {
      if (colnames(ret) == "" && ncol(ret) == 1) {
        ret <- ret[, 1, drop=TRUE]
      }
    } else {
      time(ret) <- as.Date(character())
      if (colnames(ret) == "" && ncol(ret) == 1) {
        coredata(ret) <- numeric()
      }
    }
  }
  return(ret)
}

SfitsBindWriteCache <- function( ret, more, symbol, field, cache.dir ) {
  if (is.null(ret)) {
    if (!is.null(more)) {
      ret <- more
      SfitsWriteCache(ret, symbol, field, cache.dir)
    }
  } else {
    if (NROW(more) > 0) {
      if (NROW(ret) > 0) {
        stopifnot( all.equal(tail(ret,1), head(more,1)) )
        more <- more[-1,]
      }
      ret <- rbind(ret, more)
      SfitsWriteCache(ret, symbol, field, cache.dir)
    }
  }
  return(ret)
}

SfitsCachedRead <- function( symbol, field, start, end, func, cache.dir ) {
  stopifnot( is(symbol, "character") )
  stopifnot( length(symbol) == 1 )
  stopifnot( is(field, "character") )
  stopifnot( length(field) == 1 )
  stopifnot( is(func, "function") )
  stopifnot( is(cache.dir, "character") )
  stopifnot( length(cache.dir) == 1 )

  lock <- paste(sep='', SfitsFilePath(symbol, field, cache.dir), '.lock')
  attempt <- 4
  while (!dir.create(lock, recursive=TRUE, showWarnings=FALSE)) {
    attempt <- attempt - 1
    if (attempt == 0) {
      return(PENDING_ZOO)
    }
    Sys.sleep(0.125)
  }
  on.exit(unlink(lock, recursive=TRUE))

  path <- SfitsFilePath(symbol, field, cache.dir)
  ret <- SfitsReadCache(path)
  up.to.date <- min(c(Sys.Date(), as.Date(end) + 1))
  if (!file.exists(path) || as.Date(file.mtime(path)) < up.to.date) {
    start.more <- if (is.null(ret)) NULL else stats::end(ret)
    if ( is.null(start.more) || start.more < end ) {
      more <- func(symbol, field, start.more, end)
      ret <- SfitsBindWriteCache(ret, more, symbol, field, cache.dir )
    }
  }
  ret <- ( if (is.null(ret)) NULL else window(ret,start=start,end=end) )
  return(ret)
}

SfitsCachizer <- function( source.func, data.source.name ) {
  stopifnot( is(source.func, "function") )

  function( symbol, field, start=NULL, end, cache.id ) {
    cache.dir <- SfitsCachePath(data.source.name, cache.id)
    SfitsCachedRead( symbol, field, start, end, source.func, cache.dir )
  }
}


########################
## Yahoo Data Functions

GetYahooData <- function( symbol, field, start=NULL, end )
{
  if (is.null(start)) {
    start <- as.Date("1970-01-01")
  }
  field <- tolower(field)
  colname <- switch( field,
                     open="Open",
                     high="High",
                     low="Low",
                     close="Close",
                     volume="Volume",
                     adjclose="Adj.Close",
                     dividend="Dividends")
  if (is.null(colname)) {
    warning("Field '", field, "' is not available from Yahoo!")
    return(NULL)
  }
  divflag <- field %in% c("dividend")
  url <- paste.yahoo.url( symbol, start, end, divflag )
  z <- GetWebCsvZoo(url) 
  if (is.null(z)) {
    ret <- NULL
  } else {
    if (divflag) {
      # HACK: because cache.id is not known, no cache is getting used
      close <- GetYahooData(symbol, 'close', start=start, end=end)
      if (NROW(z) > 0) {
        nz <- names(z)
        z <- merge(z, zoo(,time(close)), fill=0)
        names(z) <- nz  # work around 1.7-6 bug
      } else {
        z <- zoo(0, time(close))
      }
    }
    ret <- z[,colname]
  }
  # round in case Yahoo .csv has rounding error
  # to basis points if divs or cents otherwise
  ret <- round(ret, ifelse(divflag, 4, 2))
  return(ret)
}

ReadYahooData <- SfitsCachizer( GetYahooData, "yahoo" )

paste.yahoo.url = function( ticker, start, end, divs_only=FALSE )
{
  stopifnot( is.character(ticker) )
  stopifnot( is.logical(divs_only) )
  # yahoo periods are seconds since 1970 UTC
  paste( sep="",
    "https://query1.finance.yahoo.com/v7/finance/download/", ticker,
    "?period1=", as.numeric(as.Date(start))*24*3600,
    "&period2=", as.numeric(as.Date(end))*24*3600,
    "&interval=1d",
    "&events=", ifelse(divs_only, "div", "history"),
    "&includeAdjustedClose=true")
}

get.yahoo.prices = function( ticker, start="2000-01-01", adjust=TRUE )
{
  url = paste.yahoo.url( ticker, start, Sys.Date() )
  d = read.csv( url )
  if (adjust) { zoo( d$Adj.Close, as.Date(d$Date) ) }
  else { zoo( d$Close, as.Date(d$Date) ) }
}

get.yahoo.divs = function( ticker, start="2000-01-01" )
{
  url = paste.yahoo.url( ticker, start, Sys.Date(), TRUE )
  d = read.csv( url )
  zoo( d$Dividends, as.Date(d$Date) )
}


########################
## AlphaVantage Data Functions

paste.alphavantage.url = function( ticker, adjusted=FALSE, compact=TRUE )
{
  stopifnot( is.character(ticker) )
  stopifnot( is.logical(compact) )
  paste( sep="",
    "https://www.alphavantage.co/query",
    "?function=TIME_SERIES_DAILY", ifelse(adjusted, "_ADJUSTED", ""),
    "&symbol=", ticker,
    "&apikey=", get("alphavantage.key", envir=dataEnv),
    "&outputsize=", ifelse(compact, "compact", "full"),
    "&datatype=csv" )
}

get.alphavantage.data  <- function( symbol, field, start=NULL, end )
{
  field <- tolower(field)
  colname <- switch( field,
                     open="open",
                     high="high",
                     low="low",
                     close="close",
                     volume="volume",
                     adjclose="adjusted_close",
                     dividend="dividend_amount",
                     unsplit="split_coefficient" )
  if (is.null(colname)) {
    warning("Field '", field, "' is not available from Alpha Vantage!")
    return(NULL)
  }
  adjflag <- field %in% c("adjclose", "dividend", "unsplit")
  compact <- !is.null(start) && ((Sys.Date() - start) < 130)
  url <- paste.alphavantage.url(symbol, adjusted=adjflag, compact=compact)
  message("Sleeping ", 15, " seconds ...")
  Sys.sleep(15)
  z <- GetWebCsvZoo(url, header.regex="^timestamp,") 
  if (is.null(z)) {
    ret <- NULL
  } else {
    ret <- window(z[,colname], start=start, end=end)
  }
  return(ret)
}

read.alphavantage.data <- SfitsCachizer( get.alphavantage.data, "alpha" )

###########################################################
## Useful HTML scraping functions

get.html = function( url, size=1000000 )
{
  ret <- NULL
  tf = WebTempFile(url)
  if (!is.null(tf)) {
    ret = readChar( tf, size )
    unlink( tf )
  }
  ret
}

gregexpr.match = function( regex, txt )
{
  stopifnot( length(txt) == 1 )

  i = gregexpr( regex, txt, ignore.case=TRUE, perl=TRUE )[[1]]
  substring( txt[1], i, i + attr(i,"match.length") - 1 )
}

parse.html.elements = function( html, tagname )
{
  stopifnot( is(html,"character") )
  stopifnot( length(html) == 1 )
  stopifnot( is(tagname,"character") )
  stopifnot( length(tagname) == 1 )

  start.tag = paste( "<",tagname,"[^>]*>", sep='')
  end.tag = paste( "</",tagname,">", sep='')
  regex = paste( "(?s)",start.tag,".*?",end.tag, sep='')
  str = gregexpr.match( regex, html[1] )

  gsub( paste("(?s)^",start.tag, sep=''),
        "",
        gsub( paste( "(?s)",end.tag,"$", sep=''),
              "", str, ignore.case=TRUE, perl=TRUE ),
        ignore.case=TRUE, perl=TRUE )
}

build.uri = function( prequery, params )
{
  paste( prequery, sep="?", paste( names(params), params, sep="=", collapse="&" ) )
}

###########################################################
## Vanguard Data Functions

parse.vanguard.navs = function( html )
{
  ret <- zoo(numeric(0), as.Date(character(0)))
  tables = parse.html.elements( html, "table" )
  header = "<tr[^>]*>[^<]*<th[^>]*>\\w*Date\\w*</th>[^<]*<th[^>]*>\\w*NAV\\w*</th>.*?</tr>"
  tbody = grep( header, tables, ignore.case=TRUE, perl=TRUE, value=TRUE )
  tbody = gsub( header, "", tbody, ignore.case=TRUE, perl=TRUE )
  if (length(tbody) > 0) {
    rows = parse.html.elements( tbody[1], "tr" )
    data = sapply( rows, parse.html.elements, "td", USE.NAMES=FALSE )
    ret = zoo( as.numeric(gsub("\\$","",data[2,])), as.Date(data[1,],format="%m/%d/%Y") )
  }
  ret
}

vanguard.fund.id = function( symbol )
{
   list( VEA="0936", VGK="0963", VPL="0962",
         VT ="3141", VWO="0964", VEU="0991",
         VTI="0970", VOO="0968", VXF="0965", 
         VV ="0961", VTV="0966", VUG="0967",
         VO ="0939", VOT="0932", VOE="0935",
         VB ="0969", VBK="0938", VBR="0937",
         MGC="3137", MGK="3138", MGV="3139",
         VDE="0951", VAW="0952", VIS="0953", 
         VCR="0954", VDC="0955", VHT="0956",
         VFH="0957", VGT="0958", VOX="0959",
         VPU="0960", VIG="0920", VYM="0923",
         VNQ="0986", VSS="3184",
         VOOV="3340", VOOG="3341", IVOO="3342", IVOG="3343", IVOV="3344",
         VIOO="3345", VIOV="3346", VIOG="3347", VONE="3348", VONV="3349",
         VONG="3350", VTWO="3351", VTWV="3352", VTWG="3353", VTHR="3354",
         VNQI="3358", VXUS="3369",
         BNDX="3711", VWOB="3820",
         BND="0928", BIV="0925", BSV="0924", EDV="0930", BLV="0927",
         VCIT="3146", VGIT="3143", VCLT="3147", VGLT="3144", VMBS="3148",
         VCSH="3145", VGSH="3142", VTIP="3365"
       )[[symbol]]
}

vanguard.navs.url = function( symbol, start.date, end.date )
{
  stopifnot( is(start.date,"Date") )
  stopifnot( is(end.date,"Date") )

  fundid = vanguard.fund.id(symbol)
  build.uri( "https://personal.vanguard.com/us/funds/tools/pricehistorysearch",
    list( radio=1, results="get", FundType="ExchangeTradedShares", FundIntExt="INT",
          FundId=fundid, fundName=fundid, radiobutton2=1,
          beginDate=format( start.date, format="%m%%2F%d%%2F%Y" ),
          endDate=format( end.date, format="%m%%2F%d%%2F%Y" ),
          year="" ) )
}

read.get.navs = function( symbol, start.date, end.date )
{
  stopifnot( is(start.date,"Date") )
  stopifnot( is(end.date,"Date") )
  html = get.html( vanguard.navs.url( symbol, start.date, end.date ) )
  return(if (is.null(html)) NULL else parse.vanguard.navs(html))
}

GetVanguardNavs <- function( symbol, field, start, end ) {
  stopifnot( field == "nav" )
  stopifnot( is(end, "Date") )
  ret <- NULL
  if (is.null(start)) {
    start <- end - 365
  }
  ret <- read.get.navs( symbol, start, end )
  return(ret)
}

ReadVanguardNavs <- SfitsCachizer( GetVanguardNavs, "vanguard")

############################################
## WisdomTree data functions

ParseWisdomTreeNavs = function( html )
{
#  <table title="NAV History">
#  <thead>
#      <tr>
#          <th>Date</th>
#          <th class="right">Nav</th>
#      </tr>
#  </thead>
#  <tbody>
#  <tr class="odd">
#      <td>04/14/2014</td>
#      <td class="right">54.2354</td>
#  </tr>
#  ...
  ret <- zoo(numeric(0), as.Date(character(0)))
  tables <- parse.html.elements( html, "table" )
  if (length(tables) > 0) {
    tbody <- parse.html.elements( tables[1], "tbody" )
    if (length(tbody) > 0) {
      rows <- parse.html.elements( tbody[1], "tr" )
      data <- sapply( rows, parse.html.elements, "td", USE.NAMES=FALSE )
      ret <- zoo( as.numeric(gsub("\\$","",data[2,])), as.Date(data[1,],format="%m/%d/%Y") )
    }
  }
  ret
}

WisdomTreeID = function( symbol )
{
  list( DTN='1', DLN='2', DON='3', DHS='4', DES='15', DND='16', DNH='17',
        DXJ='18', DNL='19', DWM='20', DOL='21', DOO='22', DIM='23', DTH='24',
        DLS='25', DEW='27', DBU='28', DKA='32', DBN='36', DFE='37', DFJ='38',
        DTD='40', EXT='42', EPS='43', EZM='44', EES='45', EZY='47', DRW='49',
        DEM='50', EPI='51', DGS='53', EU='61', JYF='62', BZF='63', CYB='64',
        ICN='65', BNZ='67', SZR='68', GULF='69', CEW='70', CEW='70', ROI='71',
        HEDJ='73', CCX='74', ELD='75'
       )[[symbol]]
}

WisdomTreeNavsUrl = function( symbol )
{
  url <- "http://www.wisdomtree.com/etfs/nav-history.aspx"
  build.uri( url, list(etfid=WisdomTreeID(symbol)) )
}

GetWisdomTreeNavs <- function( symbol, field, start=NULL, end=NULL) {
  stopifnot( field == 'nav' )

  z <- ParseWisdomTreeNavs( get.html( WisdomTreeNavsUrl(symbol) ) )
  return(if (is.null(z)) NULL else window(z, start=start, end=end))
}

ReadWisdomTreeNavs <- SfitsCachizer( GetWisdomTreeNavs, "wisdomtree")

#####################################################
## General Data Getting Functions

ReadNavs <- function( symbol, field='nav', start, end, cache.id ) {
  stopifnot( field == 'nav' )
  stopifnot( length(symbol) == 1 )

  ret <- NULL
  if (!is.null(vanguard.fund.id(symbol))) {
    ret <- ReadVanguardNavs(symbol, 'nav', start, end, cache.id)
  } else if (!is.null(WisdomTreeID(symbol))) {
    ret <- ReadWisdomTreeNavs(symbol, 'nav', start, end, cache.id)
  } else {
    warning( "NAV data unknown for '", symbol, "'")
  }
  return(ret)
}

stache.exists <- function( cache.id ) {
  return(file.exists(stache.dir(cache.id)))
}

stache.create <- function( cache.id ) {
  dir <- stache.dir(cache.id)
  dir.create(dir, recursive=TRUE, showWarnings=FALSE)
  return(file.exists(dir))
}

stache.read <- function( tsips, cache.id, trim=TRUE ) {
  stopifnot( is(tsips, "character") )
  stopifnot( length(tsips) >= 1 )
  stopifnot( is(cache.id, "character") )
  stopifnot( length(cache.id) == 1 )
  stopifnot( nchar(cache.id) > 0 )

  start <- NULL
  end <- LastUTCWeekday()
  ret <- NULL
  ss <- strsplit(tsips, "/", fixed=TRUE)
  for ( i in 1:length(ss) ) {
    parts <- ss[[i]]
    if (length(parts) != 6) return(NULL)
    if (parts[1] != "") return(NULL)
    if (parts[2] != "EQ") return(NULL)
    if (parts[3] != "Q") return(NULL)
    if (parts[4] != "USD") return(NULL)
    symbol = parts[5]
    field = parts[6]
    valids <- c('OPEN', 'HIGH', 'LOW', 'CLOSE',
                'VOLUME', 'ADJCLOSE', 'DIVIDEND', 'NAV')
    if (!(field %in% valids)) {
      warning( "Field '", field, "' must be replaced with one of:\n",
               paste(collapse="\n", valids) )
      return(NULL)
    }
    field = tolower(field)
    func <- switch( field,
                    nav=ReadNavs,
                    ReadYahooData )  #default
    col <- func(symbol, field, start, end, cache.id)
    if (is.null(col)) {
      return(NULL)
    }
    if (is.pending(col)) {
      return(PENDING_ZOO)
    }
    if (NROW(col) == 0) {
      col <- zoo( as.numeric(NA), as.Date("1950-01-01") )
    }
    ret <- if (is.null(ret)) col else cbind(ret, col, all=T)
  }
  names(ret) <- tsips
  if (trim) {
    ret <- na.trim(ret)
  }
  return(ret)
}

