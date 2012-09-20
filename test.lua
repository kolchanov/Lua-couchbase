local couchbase = require('Couchbase')

function table.tostring( tbl )
		
		if type(tbl) ~= "table" then return tostring(tbl) end

		local result, done = {}, {}
		for k, v in ipairs( tbl ) do
			table.insert( result, table.val_to_str( v ) )
			done[ k ] = true
		end
		for k, v in pairs( tbl ) do
			if not done[ k ] then
				table.insert( result,
					table.key_to_str( k ) .. "=" .. table.val_to_str( v ) )
			end
		end
		return "{" .. table.concat( result, "," ) .. "}"	
	
end 

local tbs = table.tostring

local cacheHost = '172.30.1.121'
local factory = ps.cache.factory:create(cacheHost)	


function logStart (name)
	print (name .. " started ...")
end
function logPass (name)
	print (name .. " passed")
end


function testQuit ()
	logStart ('testQuit')
	local factoryQ = ps.cache.factory:create(cacheHost)	
	local key='k1'
	local cache = factoryQ:getCacheByKey (key)	
	local req = cache:quit()

	assert (req == nil)

	logPass ('testQuit')
end

function testCRUD()
	logStart ('testCRUD')

	local key ='k1'	
	local cache = factory:getCacheByKey (key)	

	local req = cache:add(key,key)
	req = cache:replace(key,key)	
	local res = cache:get(key)	

	assert(res.BODY.VALUE == key) 

	cache:set(key,100)	
	res = cache:get(key)		
	assert(tonumber(res.BODY.VALUE) == 100) 

	req = cache:delete(key)

	logPass('testCRUD')
end


function testIncrDecr()
	logStart ('testIncrDecr')
	local key = 'counter'

	local cache = factory:getCacheByKey (key)	

	local res = cache:get(key)
	local initVal = res.BODY.VALUE
	if res.HEADER.STATUS_CODE == 1 then
		initVal = -1
	end

	local req = cache:incr(key,1,0,3600)
	res = cache:get(key)
	assert (res.HEADER.STATUS_CODE == 0)
	local newVal = tonumber(res.BODY.VALUE)	
	assert(newVal == (tonumber(initVal) + 1))

	cache:decr(key,1,0,3600)
	res = cache:get(key)
	assert (res.HEADER.STATUS_CODE == 0)
	newVal = tonumber(res.BODY.VALUE)	

	assert(newVal == tonumber (initVal) or (newVal ==0 and initVal  == -1))	

	logPass ('testIncrDecr')
end

function testAppendPrepend()
	logStart ('testAppendPrepend')

	local key='forAppend'
	local cache = factory:getCacheByKey (key)	
	cache:set(key,key)	
	local res = cache:get(key)
	assert (res.BODY.VALUE == key)

	cache:append(key,"!")
	res = cache:get(key)
	assert (res.BODY.VALUE == key .. "!")

	cache:prepend(key,"!")
	res = cache:get(key)
	assert (res.BODY.VALUE == "!" .. key .. "!")	

	logPass ('testAppendPrepend')
end

function testFlush()
	logStart ('testFlush')

	local key='forFlush'
	local cache = factory:getCacheByKey (key)	
	cache:set(key,key)	
	local res = cache:get(key)
	assert (res.BODY.VALUE == key)

	local cache2 = factory:getCache()	
	cache2:flush()

	logPass ('testFlush')
end

function testCAS()
	logStart ('testCAS')


	local key='Hello'
	local cache = factory:getCacheByKey (key)	
	cache:delete(key)	
	cache:add(key,key)	
	local res = cache:get(key)
	assert (res.BODY.VALUE == key)
	local cas = res.HEADER.CAS
	cache:set(key,100, nil, cas)	
	res = cache:get(key)	
	assert(res.HEADER.STATUS_CODE == 0)
	res = cache:set(key,101, nil, cas)	
	assert(res.HEADER.STATUS_CODE == 2)

	logPass ('testCAS')
end



testCRUD()
testIncrDecr()
testAppendPrepend()
testCAS()
testFlush()
testQuit()

print ("Test Suite completed")


