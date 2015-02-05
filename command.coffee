request = require 'request'
_       = require 'lodash'
JSON3   = require 'json3'

class Command
  constructor: ->
    @sourceUri = 'http://127.0.0.1:9200'
    @sourceIndex = "skynet_trans_log_v2"
    @targetUri = 'http://127.0.0.1:9200'
    @targetIndex = "skynet_trans_log_v3"

  die: (error) =>
    console.error error
    process.exit 1

  run: =>
    @startScrolling @loop

  startScrolling: (callback=->) =>
    @startScrollingRequest  (error, response, body) =>
      return callback error if error?
      return callback body unless response.statusCode == 200
      callback null, body._scroll_id

  startScrollingRequest: (callback=->) =>
    requestParams =
      url: "#{@sourceUri}/#{@sourceIndex}/_search?search_type=scan&scroll=1m"
      method: 'POST'
      json:
        query:
          match_all: {}
        size: 1000

    request requestParams, callback

  loop: (error, scrollId) =>
    return @die error if error?

    @loopRequest scrollId, (error, response, body) =>
      return @loop error if error?
      body = JSON3.parse body
      return @loop body unless response.statusCode == 200
      scrollId = body._scroll_id
      @bulkImport body, (error) =>
        @loop error, scrollId

  loopRequest: (scrollId, callback) =>
    requestParams =
      url: "#{@sourceUri}/_search/scroll"
      method: 'GET'
      qs:
        scroll: '1m'
        scroll_id: scrollId

    request requestParams, callback

  bulkImport: (body, callback=->) =>
    @bulkImportRequest body, (error, response, body) =>
      return callback error if error?
      return callback body unless response.statusCode == 200
      callback null

  bulkImportRequest: (body, callback=->) =>
    requestParams =
      url: "#{@targetUri}/#{@targetIndex}/_bulk"
      method: 'POST'
      body: @bulkImportBody(body)

    request requestParams, callback

  bulkImportBody: (body) =>
    bulkBody = ""

    _.each body.hits.hits, (hit) =>
      bulkBody += JSON.stringify {create: {_type: "info"}}
      bulkBody += "\n"
      bulkBody += JSON.stringify hit._source
      bulkBody += "\n"

    bulkBody


command = new Command()
command.run()
