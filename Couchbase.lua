require('socket')
local pack = require("pack")


local CRC = require 'digest.crc32lua'
local bit = require("bit")
local lshift,rshift,band = bit.lshift, bit.rshift, bit.band
require('Json')
local http = require("socket.http")

local tbs = table.tostring


function hex(s)
 s=string.gsub(s,"(.)",function (x) return string.format("%02X",string.byte(x)) end)
 return s
end


bpack=string.pack
bunpack=string.unpack

--- Memcached stdtus codes
statusMap = {} 
statusMap[0] = 'No error'
statusMap[1] = 'Key not found'
statusMap[2] = 'Key exists'
statusMap[3] = 'Value too large'
statusMap[4] = 'Invalid arguments'
statusMap[5] = 'Item not stored'
statusMap[6] = 'Incr/Decr on non-numeric value'
statusMap[7] = 'NOT_MY_VBUCKET'


-- memcached command list + couchbase extension
commandMap = {
		get= 0,-- '00',
		set= 1, -- '01',
		add= 2, -- '02',		
		replace= 3, -- 03',
		delete= 4,--'04',
		increment= 5, --05',
		decrement= 6,--06',
		quit= 7, --'07',
		flush= 8, --08',
		getq= 9, --09',
		noop= 10, --'0A',
		version= 11, --'0B',
		getk= 12, -- 0C',
		getkq= 13, -- '0D',
		append= 14, --'0E',
		prepend= 15, --'0F',
		stat= 16, --'10', Not implemented
		setq= 17, --'11', Not implemented
		addq= 17, --'12', Not implemented
		replaceq= 18, --'13', Not implemented
		deleteq= 19, --'14', Not implemented
		incrementq= 20, --'15', Not implemented
		decrementq= 21, --'16', Not implemented
		quitq= 22, --'17', Not implemented
		flushq= 23, --'18', Not implemented
		appendq= 24, --'19', Not implemented
		prependq= 25, --'1A', Not implemented
		auth= 33 --'21'
	}

function getCommandDec (op)
	return commandMap[op]
end

--- Create memcached request
-- Couchbase extension - Put vBucketId into "status" field
-- @param op Oeration
-- @param key Key
-- @param value Value
-- @param expire Expire value
-- @param useExtras boolean Do use or don't extra flags
-- @param opaque 4 integer 4 byte array
-- @paran cas (Capture and Set) 8 byte array
-- @param vBucketId vBucketId identifier
-- @return Binary memcached request
function encodeRequestPack(op, key, value, expire, useExtras, opaque, cas, vBucketId, extrasValue)
	local bucket = vBucketId or 0

	local opCode = getCommandDec(op)	
	local _key = key or ''

	local _extras = {0,0,0,0} -- 4 array
	local extrasLength = 0
	if useExtras then
		if extrasValue == nil then
			-- _extras = {222,173,190,239}
			_extras = bpack(">b4",222,173,190,239)
			extrasLength = 8 -- extras value length + expire length
		else
			_extras = extrasValue
			extrasLength = _extras:len() + 4 -- extras value length + expire length
		end
	end	


	local _value = value or ''	
	local _opaque = opaque or {0,0,0,0}
	local _cas = cas or  {0,0,0,0,0,0,0,0}

	local body 
	if useExtras then
		if expire then
			-- body = bpack(">b4iA2",_extras[1],_extras[2],_extras[3],_extras[4], expire, _key, _value) 	
			body = bpack(">AiA2",_extras, expire, _key, _value) 	
		else
			-- body = bpack(">b4A2",_extras[1],_extras[2],_extras[3],_extras[4],  _key, _value) 	
			body = bpack(">A3",_extras,  _key, _value) 	
		end
	else
		if expire then
			body = bpack(">iA2", expire, _key, _value) 	
		else
			body = bpack(">A2", _key, _value) 	
		end		
	end

	return bpack(">b2hb2hib4b8A", 128, opCode, _key:len(), 
		extrasLength, 0, bucket,
		body:len(),
		_opaque[1],_opaque[2],_opaque[3],_opaque[4],
		_cas[1], _cas[2], _cas[3], _cas[4], 
		_cas[5], _cas[6], _cas[7], _cas[8],
		body)

end



--- Receive 24 byte length memcached response header
-- @param hdr 24 byte binary string
-- @return {STATUS_CODE="<Integer status code>, STATUS="<String status>, VALUE_LENGTH="<Total legth of body part>", EXTRAS_LENGTH=<Legth of extra part>, KEY_LENGTH=<Legth of key part>"}
function handleHeader(hdr)
	
	local shift, magic, opCode, keySize, extrasLength, dataType, statusCode, totalBody, opaque, cas1, cas2, cas3, cas4, cas5, cas6, cas7, cas8 = bunpack(hdr,">bbhbbhiib8")	
	return {STATUS_CODE=statusCode, STATUS=statusMap[statusCode], VALUE_LENGTH=totalBody, EXTRAS_LENGTH=extrasLength, KEY_LENGTH=keySize, OPAQUE=opaque, CAS={cas1, cas2, cas3, cas4, cas5, cas6, cas7, cas8}}		

end

--- Receive memcached response body
-- Body length calculated from header 
-- @param server LuaSocket tcp connection
-- @praram header handleHeader call result
-- @return {EXTRAS="<extras>", KEY="<key>", VALUE="<value>"}
function handleBody (server, header)

	-- if not (header.STATUS_CODE == 0 or header.STATUS_CODE == 1) then error (header.STATUS) end
	local body = ''
	if header.VALUE_LENGTH > 0 then  body = server:receive(header.VALUE_LENGTH) end

	local extras = ''	
	local extrasLength = header.EXTRAS_LENGTH or 0
	if extrasLength > 0 then extras = body:sub(1,extrasLength) end

	local key = ''
	local keyLength = header.KEY_LENGTH or 0
	if keyLength> 0 then key = body:sub(extrasLength + 1, extrasLength + keyLength) end	

	local value  = ''
	local valueLength = header.VALUE_LENGTH - (header.EXTRAS_LENGTH or 0) - (header.KEY_LENGTH or 0)	

	if valueLength > 0 then value = body:sub(extrasLength + keyLength +1, extrasLength + keyLength + valueLength) end	

	return {KEY=key, VALUE=value, EXTRAS=extras}

end	



function formatAdd(key, value, expire, vBucketId)
	expire = expire or 0
	return encodeRequestPack("add", tostring(key), value, expire, true, nil, nil, vBucketId)	
end

function formatSet(key, value, expire, vBucketId, cas)
	expire = expire or 0
	return encodeRequestPack("set", tostring(key), value, expire, true, nil, cas, vBucketId)	
end

function formatReplace(key, value, expire, vBucketId, cas)
	expire = expire or 0
	return encodeRequestPack("replace", tostring(key), value, expire, true, nil, cas, vBucketId)	
end

function formatGet (key, vBucketId)
	return encodeRequestPack('get', key, nil, nil, nil, nil, nil, vBucketId)
end

function formatDelete (key, vBucketId, cas)
	return encodeRequestPack('delete', key, nil, nil, nil, nil, cas, vBucketId)
end


function formatAppend(key, value,  vBucketId, cas)
	return encodeRequestPack("append", tostring(key), value, nil, nil, nil, cas, vBucketId)	
end

function formatPrepend(key, value,  vBucketId, cas)
	return encodeRequestPack("prepend", tostring(key), value, nil, nil, nil, cas, vBucketId)	
end


function formatSasl (name, pass)
	return encodeRequestPack('auth', 'PLAIN', '\0' .. name .. '\0' ..  pass)
end

function formatFlush ()
	return encodeRequestPack('flush')
end

function formatIncr(key, aVal, iVal, expire, vBucketId)
	local _aVal = aVal or 1
	local _iVal = iVal or 0
	
	local extrasVal = bpack(">b4ib4i", 0,0,0,0, _aVal, 0,0,0,0, _iVal) 		 
	return  encodeRequestPack("increment", key, nil, expire, true, nil, nil, vBucketId, extrasVal)	
end

function formatDecr(key, aVal, iVal, expire, vBucketId)
	local _aVal = aVal or 1
	local _iVal = iVal or 0
	
	local extrasVal = bpack(">b4ib4i", 0,0,0,0, _aVal, 0,0,0,0, _iVal) 		 	
	return  encodeRequestPack("decrement", key, nil, expire, true, nil, nil, vBucketId, extrasVal)	
end

function formatNoop()
	return encodeRequestPack('noop')
end

function formatQuit()
	return encodeRequestPack('quit')
end


ps = ps or {}
ps.cache = ps.cache or {}
ps.cache.couchbase = {}

ps.cache.couchbase.cbmap={}


function ps.cache.couchbase:connect (host,port,cacheName, pass, timeout, vBucketAware, serverList, vBucketMap)

	local self ={
		HOST=host,
		PORT=port,
		CACHE_NAME=cachename,
		PASS=pass,
		SERVER=nil,
		VBUCKETAWARE=vBucketAware or false,
		SERVER_LIST=serverList,
		VBUCKET_MAP=vBucketMap,		
		TIMEOUT=timeout or 30
	}

	local srv, err
	-- local srv, err = socket.connect(host, port, lhost, lport)
	local key = host .. ":" .. port
	if ps.cache.couchbase.cbmap[key] ~= nil then
		self.SERVER = ps.cache.couchbase.cbmap[key]
	else
		srv, err = socket.connect(host, port)
		if srv == nil then
			error (err)
		end
		self.SERVER = srv		
		ps.cache.couchbase.cbmap[key] = srv
	end

	
	if self.VBUCKET_MAP then
		self.VBUCKET_MAP_LEN = # self.VBUCKET_MAP
	end

	function handleRequest (str)

		self.SERVER:send(str)		
		local hdr = self.SERVER:receive(24)

		local header = handleHeader(hdr)				
		local body = handleBody(self.SERVER, header)		
		return {HEADER=header,BODY=body}
	end

	function getBucketByKey (key)
		local crc = CRC.crc32 (key)		
		local vBucketId = band(rshift(crc,16), self.VBUCKET_MAP_LEN-1)	
		-- print ("-----------------")	
		-- print(vBucketId, crc, rshift(crc,16),(self.VBUCKET_MAP_LEN-1))
		return vBucketId
	end

	function self:sasl (name, pass)
		local str = formatSasl (name, pass)
		local resp = handleRequest (str)
		if resp.BODY == nil or resp.BODY.VALUE ~= 'Authenticated' then
			error ('Failed connect to bucket ' .. name)
		end		
	end		

	function self:noop()
		local str = formatNoop()
		local resp = handleRequest (str)		
	end

	function self:quit()
		local str = formatQuit()
		local resp = handleRequest (str)		
	end	

	function self:flush()
		local str = formatFlush()
		local resp = handleRequest (str)		
	end

	function self:set (key, value, expire, cas)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatSet(key, value, expire, vBucketId, cas)		
		return handleRequest (str)
	end		

	function self:add (key, value, expire)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatAdd(key, value, expire, vBucketId, cas)		
		return handleRequest (str)
	end		

	function self:replace (key, value, expire, cas)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatReplace(key, value, expire, vBucketId, cas)		
		return handleRequest (str)
	end	


	function self:get (key)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end
		local str = formatGet(key, vBucketId)		
		local ret = handleRequest (str)		
		return ret
	end

	function self:delete (key, cas)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end
		local str = formatDelete(key, vBucketId, cas)		
		local ret = handleRequest (str)		
		return ret
	end


	function self:incr (key, aVal, iVal, expire)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatIncr(key, aVal, iVal, expire, vBucketId)

		return handleRequest (str)
	end	


	function self:decr (key, aVal, iVal, expire)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatDecr(key, aVal, iVal, expire, vBucketId)

		return handleRequest (str)
	end	


	function self:append (key, value, cas)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatAppend(key, value, vBucketId, cas)						
		return handleRequest (str)
	end

	function self:prepend (key, value, cas)
		local vBucketId = 0
		if self.VBUCKETAWARE then
			vBucketId = getBucketByKey (key)
		end		
		local str = formatPrepend(key, value, vBucketId, cas)						
		return handleRequest (str)
	end	

	function self:close()
		self.SERVER:close()
	end

	if cacheName ~= nil and pass ~= nill then		
		self:sasl (cacheName, pass)
	end

	self.SERVER:settimeout(self.TIMEOUT)
	-- self.SERVER:setoption('tcp-nodelay', true)

	return setmetatable(self, {})
end

ps.cache.factory = ps.cache.factory or {}

function ps.cache.factory:create (host, cacheName, pass, timeout)
	assert (host, "Host name must be not null")
	cacheName = cacheName or 'default'
	local self ={
		HOST=host,
		CACHE_NAME=cacheName,
		PASS=pass,
		TIMEOUT=timeout or 30,		
		CASHMAP={}
	}	


	function getRestInfo(host, cacheName, pass)
		local user = ''
		local pswd = ''
		if cacheName ~= nil and cacheName ~= 'default' then 
			assert(pass)
			user = cacheName
			pswd = ':' .. pass .. '@' 			
		end

		local b, c, h = http.request("http://" .. user .. pswd .. self.HOST .. ":8091/pools/default/buckets/"..cacheName)
		assert (c, 200)
		return b
	end

	local data = Json.Decode(getRestInfo(host,cacheName, pass))


	self.VMAP = data.vBucketServerMap.vBucketMap
	self.VMAP_LEN = # self.VMAP

	self.SERVER_LIST = data.vBucketServerMap.serverList
	self.SERVER_LIST_LEN = # self.SERVER_LIST

	function auxF (ar, pos)		
		return self.SERVER_LIST[ar[1]+1]
	end		

	function self:getBucketByKey (key)
		local crc = CRC.crc32 (key)		
		local vBucketId = band(rshift(crc,16), self.VMAP_LEN-1)		
		return vBucketId
	end

	function getHostByKey (key)
		local vBucketId = self:getBucketByKey (key) + 1
		local mp = self.VMAP[vBucketId]
		local srv = auxF (mp,1)

		return srv		
	end

	function self:getHostByKey2 (key)
		local vBucketId = self:getBucketByKey (key) + 1
		local mp = self.VMAP[vBucketId]
		local srv = auxF (mp,1)

		return srv		
	end	

	function split(str, sep)
        local sep, fields = sep or ":", {}
        local pattern = string.format("([^%s]+)", sep)
        str:gsub(pattern, function(c) fields[#fields+1] = c end)
        return fields
	end	

	--- param host HOST:PORT parameter
	function self:getByHost (host)
		local par = split (host)
		local cache = ps.cache.couchbase:connect (par[1],par[2],self.CACHE_NAME,self.PASS, self.TIMEOUT, true, self.SERVER_LIST, self.VMAP)
		return cache		
	end	

	function self:getCacheByKey(key)
		local srv = getHostByKey (key)
		return self:getByHost (srv)
	end

	function self:getCache()
		local srv = getHostByKey ('0')
		return self:getByHost (srv)
	end

	return setmetatable(self, {})
end

