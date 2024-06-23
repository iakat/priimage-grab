local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_album = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_count = 0
local bad_items = {}
local ids = {}

local thread_counts = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    discovered_count = discovered_count + 1
    if discovered_count == 1000 then
      submit_backfeed()
      discovered_count = 0
    end
    return true
  end
  return false
end

find_item = function(url)
  local value = nil
  local value2 = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https://prcm%.jp/album/([^/%?&;]+)/pic/([0-9]+)$"]="pic",
    ["^https://prcm%.jp/album/([^/%?&;]+)$"]="album",
    ["^https://prcm%.jp/user/([^/%?&;]+)$"]="user",
    ["^https://prcm%.jp/list/([^/%?&;]+)$"]="list",
    ["^https://(prof%.prepics%-cdn%.com/.+)$"]="cdn",
    ["^https://(pics%.prcm%.jp/.+)$"]="cdn",
    ["^https://(img%.prepics%.com/.+)$"]="cdn",
    ["^https://prcm%.jp/talk/([0-9]+)$"]="talk",
    ["^https://prcm%.jp/talk/list/([^/%?&;]+)$"]="talk-list",
    ["^https://news%.prcm%.jp/article/([0-9]+)$"]="article"
  }) do
    if name == "pic" then
      value2, value = string.match(url, pattern)
    else
      value = string.match(url, pattern)
    end
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["value2"]=value2,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_album = nil
    if item_type == "pic" then
      item_album = found["value2"]
      item_name_new = item_type .. ":" .. item_album .. ":" .. item_value
    else
      item_name_new = item_type .. ":" .. item_value
    end
    if item_name_new ~= item_name then
      ids = {}
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      is_new_design = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if string.match(url, "^https?://[^/]*prcm%.jp/sign%-in%?")
    or string.match(url, "%?sort=[0-9]")
    or string.match(url, "%?order=")
    or string.match(url, "/album/[^/]+/follow$") then
    return false
  end

  if item_type == "pic" then
    local temp_id = string.match(url, "^https?://prcm%.jp/pic/original%-image/id/([0-9a-zA-Z]+)$")
    if temp_id then
      ids[string.lower(temp_id)] = true
    end
  end

  local skip = false
  for pattern, type_ in pairs({
    ["^https?://prcm%.jp/album/([^/%?&;]+/pic/[0-9]+)$"]="pic",
    ["^https?://prcm%.jp/album/([^/%?&;]+)$"]="album",
    ["^https?://prcm%.jp/user/([^/%?&;]+)$"]="user",
    ["^https?://prcm%.jp/list/([^/%?&;]+)$"]="list",
    ["^https?://(prof%.prepics%-cdn%.com/.+)$"]="cdn",
    ["^https?://(pics%.prcm%.jp/.+)$"]="cdn",
    ["^https?://(img%.prepics%.com/.+)$"]="cdn",
    ["^https?://prcm%.jp/talk/list/([^/%?&;]+)$"]="talk-list"
  }) do
    match = string.match(url, pattern)
    if match then
      if type_ == "pic" then
        match = string.gsub(match, "/pic/", ":")
      end
      if type_ == "talk-list" then
        discover_item(discovered_items, "list:" .. match)
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        if type_ == "album" and item_album == match then
          skip = false
        else
          skip = true
        end
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://prcm%.jp/")
    and not string.match(url, "^https?://news%.prcm%.jp/")
    and not string.match(url, "^https?://prof%.prepics%-cdn%.com/")
    and not string.match(url, "^https?://pics%.prcm%.jp/")
    and not string.match(url, "^https?://img%.prepics%.com/") then
    discover_item(discovered_outlinks, url)
    return false
  end

  for _, pattern in pairs({
    "([a-z0-9A-Z%%%._%-]+)",
    "([0-9]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    newurl = percent_encode_url(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, {
        url=url_
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return check_new_params(newurl, param, tostring(value))
    else
      return check_new_params(newurl, param, default)
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if allowed(url)
    and status_code < 300 then
    html = read_file(file)
    if item_type == "pic" then
      check("https://prcm.jp/embed/pic-good-list?gazo_id=" .. item_value)
    end
    if item_type == "talk" then
      check("https://prcm.jp/talk/" .. item_value .. "/about")
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  is_new_design = false
  if http_stat["len"] == 0 then
    io.stdout:write("Zero length response.\n")
    io.stdout:flush()
    retry_url = true
    return false
  end
  if http_stat["statcode"] == 302
    and url["url"] ~= urlparse.absolute(url["url"], http_stat["newloc"]) then
    io.stdout:write("Unexpected 302.\n")
    io.stdout:flush()
    retry_url = true
    return false
  elseif http_stat["statcode"] == 200
    and string.match(url["url"], "^https?://[^/]*prcm%.jp/")
    and not string.match(url["url"], "^https?://pics%.prcm%.jp/") then
    local html = read_file(http_stat["local_file"])
    if string.match(html, "[iI]ncapsula%s+incident")
      or string.match(html, '<[mM][eE][tT][aA] [nN][aA][mM][eE]="[rR][oO][bB][oO][tT][sS]" [cC][oO][nN][tT][eE][nN][tT]="[nN][oO][iI][nN][dD][eE][xX],%s*[nN][oO][fF][oO][lL][lL][oO][wW]">')
      or string.match(html, "<body>%s*</body>")
      or not string.match(html, "prcm%.jp/") then
      io.stdout:write("Possible 200 page with captcha.\n")
      io.stdout:flush()
      retry_url = true
      return false
    end
  elseif http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 302 then
    io.stdout:write("Bad status code.\n")
    io.stdout:flush()
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  
  if is_new_design then
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if (processed(newloc) or not allowed(newloc, url["url"]))
      and not url["url"] == newloc then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if seen_200[url["url"]] then
    print("Received data incomplete.")
    abort_item()
    return wget.actions.EXIT
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 8
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      seen_200[url["url"]] = true
    end
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

submit_backfeed = function()
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end
  for key, data in pairs({
    ["priimage-neleobh4n40cb9x6"] = discovered_items,
    ["urls-ecv7j96zbty4tbuy"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 500 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
  discovered_items = {}
  discovered_outlinks = {}
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  submit_backfeed()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


