// NodeMBTCP - MBTCPPROTOCOL - A library for communication using Modbus/TCP from node.js.

var net = require("net");
var _ = require("underscore");
var util = require("util");
var crc16 = require("./crc16");
var effectiveDebugLevel = 0; // intentionally global, shared between connections

module.exports = NodeMBTCP;

function NodeMBTCP(){
	var self = this;

	// This header is used to assemble a read packet.
	self.Header = Buffer.from([0xff,0xff,0x00,0x00,0x00,0x00]);// followed by function and then data.  0,1 = trans ID, 2,3 = always 0, 4,5 = length, 6 = unit ID, 7=function, 8+=data

	self.readReq = Buffer.alloc(1500);
	self.writeReq = Buffer.alloc(1500);

	self.resetPending = false;
	self.resetTimeout = undefined;

	self.sendpdu = false;
	self.maxPDU = 255;
	self.isoclient = undefined;
	self.isoConnectionState = 0;
	self.requestMaxParallel = 1;
	self.maxParallel = 1;
	self.parallelJobsNow = 0;
	self.maxGap = 5;
	self.doNotOptimize = false;
	self.connectCallback = undefined;
	self.readDoneCallback = undefined;
	self.writeDoneCallback = undefined;
	self.connectTimeout = undefined;
	self.PDUTimeout = undefined;
	self.globalTimeout = 4500;
	self.badReadCounter = 0;
	self.badWriteCounter = 0;

	self.readPacketArray = [];
	self.writePacketArray = [];
	self.polledReadBlockList = [];
	self.instantWriteBlockList = [];
	self.globalReadBlockList = [];
	self.globalWriteBlockList = [];
	self.masterSequenceNumber = 1;
	self.translationCB = doNothing;
	self.connectionParams = undefined;
	self.connectionID = 'UNDEF';
	self.addRemoveArray = [];
	self.readPacketValid = false;
	self.writeInQueue = false;
	self.connectCBIssued = false;
	self.dropConnectionCallback = null;
	self.dropConnectionTimer = null;

// EIP specific
	self.sessionHandle = 0; // Define as zero for when we write packets prior to connection

	self.defaultID = 1;
}

NodeMBTCP.prototype.setTranslationCB = function(cb) {
	var self = this;
	if (typeof cb === "function") {
		outputLog('Translation OK',1,self.connectionID);
		self.translationCB = cb;
	}
}

NodeMBTCP.prototype.initiateConnection = function (cParam, callback) {
	var self = this;
	if (cParam === undefined) { cParam = {port: 502, host: '192.168.8.106', RTU: false}; }
	outputLog('Initiate Called - Connecting to PLC with address and parameters:');
	outputLog(cParam);
	if (typeof(cParam.name) === 'undefined') {
		self.connectionID = cParam.host;
	} else {
		self.connectionID = cParam.name;
	}
	if (typeof(cParam.defaultID) != 'undefined') {
		self.defaultID = cParam.defaultID;
	}
	self.connectionParams = cParam;
	self.connectCallback = callback;
	self.connectCBIssued = false;
	self.connectNow(self.connectionParams, false);
}

NodeMBTCP.prototype.dropConnection = function (callback) {
	var self = this;
	if (typeof(self.isoclient) !== 'undefined') {
		// store the callback and request and end to the connection
		self.dropConnectionCallback = callback;
		self.isoclient.end();
		// now wait for 'on close' event to trigger connection cleanup

		// but also start a timer to destroy the connection in case we do not receive the close
		self.dropConnectionTimer = setTimeout(function() {
			if (self.dropConnectionCallback) {
				// destroy the socket connection
				self.isoclient.destroy();
				// clean up the connection now the socket has closed
				self.connectionCleanup();
				// initate the callback
				self.dropConnectionCallback();
				// prevent any possiblity of the callback being called twice
				self.dropConnectionCallback = null;
			}
		}, 2500);
	} else {
		// if client not active, then callback immediately
		callback();
	}
}

NodeMBTCP.prototype.connectNow = function(cParam, suppressCallback) { // TODO - implement or remove suppressCallback
	var self = this;
	// Don't re-trigger.
	if (self.isoConnectionState >= 1) { return; }
	self.connectionCleanup();
	self.isoclient = net.connect(cParam, function(){
		self.onTCPConnect.apply(self,arguments);
	});

	self.isoConnectionState = 1;  // 1 = trying to connect

	self.isoclient.on('error', function(){
		self.connectError.apply(self, arguments);
	});

	outputLog('MODBUS <initiating a new connection ' + Date() + ' >',1,self.connectionID);
	outputLog('Attempting to connect to host...',0,self.connectionID);
}

NodeMBTCP.prototype.connectError = function(e) {
	var self = this;

	// Note that a TCP connection timeout error will appear here.  An ISO connection timeout error is a packet timeout.
	outputLog('We Caught a connect error ' + e.code,0,self.connectionID);
	if ((!self.connectCBIssued) && (typeof(self.connectCallback) === "function")) {
		self.connectCBIssued = true;
		self.connectCallback(e);
	}
	self.isoConnectionState = 0;
}

NodeMBTCP.prototype.readWriteError = function(e) {
	var self = this;
	outputLog('We Caught a read/write error ' + e.code + ' - resetting connection',0,self.connectionID);
	self.isoConnectionState = 0;
	self.connectionReset();
}

NodeMBTCP.prototype.packetTimeout = function(packetType, packetSeqNum) {
	var self = this;
	outputLog('PacketTimeout called with type ' + packetType + ' and seq ' + packetSeqNum,1,self.connectionID);
/*	if (packetType === "connect") {
		outputLog("TIMED OUT waiting for EIP Connection Response from the PLC - Disconnecting",0,self.connectionID);
		outputLog("Wait for 2 seconds then try again.",0,self.connectionID);
		self.connectionReset();
		outputLog("Scheduling a reconnect from packetTimeout, connect type",0,self.connectionID);
		setTimeout(function(){
			outputLog("The scheduled reconnect from packetTimeout, connect type, is happening now",0,self.connectionID);
			self.connectNow.apply(self,arguments);
		}, 2000, connectionParams);
		return undefined;
	} */
	if (packetType === "read") {
		outputLog("READ TIMEOUT on sequence number " + packetSeqNum,0,self.connectionID);
		if (self.isoConnectionState === 4) { // Reset before calling readResponse so ResetNow will take place this cycle
			outputLog("ConnectionReset from read packet timeout.", 0, self.connectionID);
			self.badReadCounter += 1;
			if (self.badReadCounter > (10+self.readPacketArray.length-1)) {
				self.connectionReset();
				self.badReadCounter = 0;
			}
		}
		self.readResponse(undefined, self.findReadIndexOfSeqNum(packetSeqNum));
		return undefined;
	}
	if (packetType === "write") {
		outputLog("WRITE TIMEOUT on sequence number " + packetSeqNum,0,self.connectionID);
		if (self.isoConnectionState === 4) { // Reset before calling writeResponse so ResetNow will take place this cycle
			outputLog("ConnectionReset from write packet timeout.", 0, self.connectionID);
			self.badWriteCounter += 1;
			if (self.badWriteCounter > (10+self.writePacketArray.length-1)) {
				self.connectionReset();
				self.badWriteCounter = 0;
			}
		}
		self.writeResponse(undefined, self.findWriteIndexOfSeqNum(packetSeqNum));
		return undefined;
	}
	outputLog("Unknown timeout error.  Nothing was done - this shouldn't happen.",0,self.connectionID);
}

NodeMBTCP.prototype.onTCPConnect = function() {
	var self = this;
	outputLog('MODBUS TCP Connection Established to ' + self.isoclient.remoteAddress + ' on port ' + self.isoclient.remotePort,0,self.connectionID);

	// Track the connection state
	self.isoConnectionState = 4;  // 4 = all connected, simple with Modbus

	self.isoclient.removeAllListeners('data');
	self.isoclient.removeAllListeners('error');

	self.isoclient.on('data', function() {
		self.onResponse.apply(self, arguments);
	});  // We need to make sure we don't add this event every time if we call it on data.
	self.isoclient.on('error', function() {
		self.readWriteError.apply(self, arguments);
	});  // Might want to remove the connecterror listener
	// Hook up the event that fires on disconnect
	self.isoclient.on('end', function() {
		self.onClientDisconnect.apply(self, arguments);
	});
    // listen for close (caused by us sending an end)
	self.isoclient.on('close', function() {
		self.onClientClose.apply(self, arguments);
	});
	if ((!self.connectCBIssued) && (typeof(self.connectCallback) === "function")) {
		self.connectCBIssued = true;
		self.connectCallback();
	}
	return;
}

NodeMBTCP.prototype.writeItems = function(arg, value, cb) {
	var self = this;
	var i;
	outputLog("Preparing to WRITE " + arg,0,self.connectionID);

	if (self.isWriting()) {
		outputLog("You must wait until all previous writes have finished before scheduling another. ",0,self.connectionID);
		return;
	}

	if (typeof cb === "function") {
		self.writeDoneCallback = cb;
	} else {
		self.writeDoneCallback = doNothing;
	}

	self.instantWriteBlockList = []; // Initialize the array.

	if (typeof arg === "string") {
		self.instantWriteBlockList.push(stringToMBAddr(self.translationCB(arg), arg, self.defaultID, self.connectionID));
		if (typeof(self.instantWriteBlockList[self.instantWriteBlockList.length - 1]) !== "undefined") {
			self.instantWriteBlockList[self.instantWriteBlockList.length - 1].writeValue = value;
		}
	} else if (_.isArray(arg) && _.isArray(value) && (arg.length == value.length)) {
		for (i = 0; i < arg.length; i++) {
			if (typeof arg[i] === "string") {
				self.instantWriteBlockList.push(stringToMBAddr(self.translationCB(arg[i]), arg[i], self.defaultID, self.connectionID));
				if (typeof(self.instantWriteBlockList[self.instantWriteBlockList.length - 1]) !== "undefined") {
					self.instantWriteBlockList[self.instantWriteBlockList.length - 1].writeValue = value[i];
				}
			}
		}
	}

	// Validity check.
	for (i=self.instantWriteBlockList.length-1;i>=0;i--) {
		if (self.instantWriteBlockList[i] === undefined) {
			self.instantWriteBlockList.splice(i,1);
			outputLog("Dropping an undefined write item",0,self.connectionID);
		}
	}
	self.prepareWritePacket();
	if (!self.isReading()) {
		self.sendWritePacket();
	} else {
		self.writeInQueue = true;
	}
}

NodeMBTCP.prototype.findItem = function(useraddr) {
	var self = this;
	var i;
	for (i = 0; i < self.polledReadBlockList.length; i++) {
		if (self.polledReadBlockList[i].useraddr === useraddr) { return self.polledReadBlockList[i]; }
	}
	return undefined;
}

NodeMBTCP.prototype.addItems = function(arg) {
	var self = this;
	self.addRemoveArray.push({arg: arg, action: 'add'});
}

NodeMBTCP.prototype.addItemsNow = function(arg) {
	var self = this;
	var i;
	outputLog("Adding " + arg,0,self.connectionID);
	addItemsFlag = false;
	if (typeof arg === "string") {
		self.polledReadBlockList.push(stringToMBAddr(self.translationCB(arg), arg, self.defaultID, self.connectionID));
	} else if (_.isArray(arg)) {
		for (i = 0; i < arg.length; i++) {
			if (typeof arg[i] === "string") {
				self.polledReadBlockList.push(stringToMBAddr(self.translationCB(arg[i]), arg[i], self.defaultID, self.connectionID));
			}
		}
	}

	// Validity check.
	for (i=self.polledReadBlockList.length-1;i>=0;i--) {
		if (self.polledReadBlockList[i] === undefined) {
			self.polledReadBlockList.splice(i,1);
			outputLog("Dropping an undefined request item.",0,self.connectionID);
		}
	}
//	prepareReadPacket();
	self.readPacketValid = false;
}

NodeMBTCP.prototype.removeItems = function(arg) {
	var self = this;
	self.addRemoveArray.push({arg : arg, action: 'remove'});
}

NodeMBTCP.prototype.removeItemsNow = function(arg) {
	var self = this;
	var i;
	self.removeItemsFlag = false;
	if (typeof arg === "undefined") {
		self.polledReadBlockList = [];
	} else if (typeof arg === "string") {
		for (i = 0; i < self.polledReadBlockList.length; i++) {
			outputLog('TCBA ' + self.translationCB(arg),0,self.connectionID);
			if (self.polledReadBlockList[i].addr === self.translationCB(arg)) {
				outputLog('Splicing',1,self.connectionID);
				self.polledReadBlockList.splice(i, 1);
			}
		}
	} else if (_.isArray(arg)) {
		for (i = 0; i < self.polledReadBlockList.length; i++) {
			for (j = 0; j < arg.length; j++) {
				if (self.polledReadBlockList[i].addr === self.translationCB(arg[j])) {
					self.polledReadBlockList.splice(i, 1);
				}
			}
		}
	}
	self.readPacketValid = false;
	//	prepareReadPacket();
}

NodeMBTCP.prototype.readAllItems = function(arg) {
	var self = this;
	var i;

	outputLog("Reading All Items (readAllItems was called)",1,self.connectionID);

	if (typeof arg === "function") {
		self.readDoneCallback = arg;
	} else {
		self.readDoneCallback = doNothing;
	}

	if (self.isoConnectionState !== 4) {
		outputLog("Unable to read when not connected. Return bad values.",0,self.connectionID);
	} // For better behaviour when auto-reconnecting - don't return now

	// Check if ALL are done...  You might think we could look at parallel jobs, and for the most part we can, but if one just finished and we end up here before starting another, it's bad.
	if (self.isWaiting()) {
		outputLog("Waiting to read for all R/W operations to complete.  Will re-trigger readAllItems in 100ms.",0,self.connectionID);
		setTimeout(function() {
			self.readAllItems.apply(self, arguments);
		}, 100, arg);
		return;
	}

	// Now we check the array of adding and removing things.  Only now is it really safe to do this.
	self.addRemoveArray.forEach(function(element){
		outputLog('Adding or Removing ' + util.format(element), 1, self.connectionID);
		if (element.action === 'remove') {
			self.removeItemsNow(element.arg);
		}
		if (element.action === 'add') {
			self.addItemsNow(element.arg);
		}
	});

	self.addRemoveArray = []; // Clear for next time.

	if (!self.readPacketValid) { self.prepareReadPacket(); }

	// ideally...  incrementSequenceNumbers();

	outputLog("Calling SRP from RAI",1,self.connectionID);
	self.sendReadPacket(); // Note this sends the first few read packets depending on parallel connection restrictions.
}

NodeMBTCP.prototype.isWaiting = function() {
	var self = this;
	return (self.isReading() || self.isWriting());
}

NodeMBTCP.prototype.isReading = function() {
	var self = this;
	var i;
	// Walk through the array and if any packets are marked as sent, it means we haven't received our final confirmation.
	for (i=0; i<self.readPacketArray.length; i++) {
		if (self.readPacketArray[i].sent === true) { return true };
	}
	return false;
}

NodeMBTCP.prototype.isWriting = function() {
	var self = this;
	var i;
	// Walk through the array and if any packets are marked as sent, it means we haven't received our final confirmation.
	for (i=0; i<self.writePacketArray.length; i++) {
		if (self.writePacketArray[i].sent === true) { return true };
	}
	return false;
}


NodeMBTCP.prototype.clearReadPacketTimeouts = function() {
	var self = this;
	outputLog('Clearing read PacketTimeouts',1,self.connectionID);
	// Before we initialize the readPacketArray, we need to loop through all of them and clear timeouts.
	for (i=0;i<self.readPacketArray.length;i++) {
		clearTimeout(self.readPacketArray[i].timeout);
		self.readPacketArray[i].sent = false;
		self.readPacketArray[i].rcvd = false;
	}
}

NodeMBTCP.prototype.clearWritePacketTimeouts = function() {
	var self = this;
	outputLog('Clearing write PacketTimeouts',1,self.connectionID);
	// Before we initialize the readPacketArray, we need to loop through all of them and clear timeouts.
	for (i=0;i<self.writePacketArray.length;i++) {
		clearTimeout(self.writePacketArray[i].timeout);
		self.writePacketArray[i].sent = false;
		self.writePacketArray[i].rcvd = false;
	}
}

NodeMBTCP.prototype.prepareWritePacket = function() {
	var self = this;
	var itemList = self.instantWriteBlockList;
	var requestList = [];			// The request list consists of the block list, split into chunks readable by PDU.
	var requestNumber = 0;
	var itemsThisPacket;
	var numItems;

	// Sort the items using the sort function, by type and offset.
	itemList.sort(itemListSorter);

	// Just exit if there are no items.
	if (itemList.length == 0) {
		return undefined;
	}

	// At this time we do not do write optimizations.
	// The reason for this is it is would cause numerous issues depending how the code was written in the PLC.
	// If we write B3:0/0 and B3:0/1 then to optimize we would have to write all of B3:0, which also writes /2, /3...
	//
	// I suppose when working with integers, we could write these as one block.
	// But if you really, really want the program to do that, write an array yourself and it will.
	self.globalWriteBlockList[0] = itemList[0];
	self.globalWriteBlockList[0].itemReference = [];
	self.globalWriteBlockList[0].itemReference.push(itemList[0]);

	var thisBlock = 0;
	itemList[0].block = thisBlock;
	var maxByteRequest = 244; // 4*Math.floor((self.maxPDU - 18 - 12)/4);  // Absolutely must not break a real array into two requests.  Maybe we can extend by two bytes when not DINT/REAL/INT.
//	outputLog("Max Write Length is " + maxByteRequest);

	// Just push the items into blocks and figure out the write buffers
	for (i=0;i<itemList.length;i++) {
		self.globalWriteBlockList[i] = itemList[i]; // Remember - by reference.
		self.globalWriteBlockList[i].isOptimized = false;
		self.globalWriteBlockList[i].itemReference = [];
		self.globalWriteBlockList[i].itemReference.push(itemList[i]);
		bufferizeMBItem(itemList[i],self.connectionID);
//		outputLog("Really Here");
	}

//	outputLog("itemList0 wb 0 is " + itemList[0].writeBuffer[0] + " gwbl is " + globalWriteBlockList[0].writeBuffer[0]);

	var thisRequest = 0;

	// Split the blocks into requests, if they're too large.
	for (i=0;i<self.globalWriteBlockList.length;i++) {
		var startElement = self.globalWriteBlockList[i].offset;
		var remainingLength = self.globalWriteBlockList[i].byteLength;
		var remainingTotalArrayLength = self.globalWriteBlockList[i].totalArrayLength;
		var lengthOffset = 0;

		// Always create a request for a globalReadBlockList.
		requestList[thisRequest] = self.globalWriteBlockList[i].clone();

		// How many parts?
		self.globalWriteBlockList[i].parts = Math.ceil(self.globalWriteBlockList[i].byteLength/maxByteRequest); // This is still true for modbus coils
//		outputLog("globalWriteBlockList " + i + " parts is " + globalWriteBlockList[i].parts + " offset is " + globalWriteBlockList[i].offset + " MBR is " + maxByteRequest);

		self.globalWriteBlockList[i].requestReference = [];

		// If we need to spread the sending/receiving over multiple packets...
		for (j=0;j<self.globalWriteBlockList[i].parts;j++) {
			requestList[thisRequest] = self.globalWriteBlockList[i].clone();
			self.globalWriteBlockList[i].requestReference.push(requestList[thisRequest]);
			requestList[thisRequest].offset = startElement;
			requestList[thisRequest].byteLength = Math.min(maxByteRequest,remainingLength);
			requestList[thisRequest].totalArrayLength = Math.min(maxByteRequest*8,remainingLength*8,self.globalWriteBlockList[i].totalArrayLength);
			requestList[thisRequest].byteLengthWithFill = requestList[thisRequest].byteLength;
			if (requestList[thisRequest].byteLengthWithFill % 2) { requestList[thisRequest].byteLengthWithFill += 1; };

			// max
			//outputLog("LO " + lengthOffset + " rblf " + requestList[thisRequest].byteLengthWithFill + " val " + globalWriteBlockList[i].writeBuffer[0]);
			requestList[thisRequest].writeBuffer = self.globalWriteBlockList[i].writeBuffer.slice(lengthOffset, lengthOffset + requestList[thisRequest].byteLengthWithFill);
			requestList[thisRequest].writeQualityBuffer = self.globalWriteBlockList[i].writeQualityBuffer.slice(lengthOffset, lengthOffset + requestList[thisRequest].byteLengthWithFill);
			lengthOffset += self.globalWriteBlockList[i].requestReference[j].byteLength;

			if (self.globalWriteBlockList[i].parts > 1) {
				requestList[thisRequest].datatype = 'BYTE';
				requestList[thisRequest].dtypelen = 1;
				requestList[thisRequest].arrayLength = requestList[thisRequest].byteLength;//globalReadBlockList[thisBlock].byteLength;		(This line shouldn't be needed anymore - shouldn't matter)
			}
			remainingLength -= maxByteRequest;
			if (self.globalWriteBlockList[i].startRegister > 20000) {
//				startElement += maxByteRequest/requestList[thisRequest].multidtypelen; // I believe we want to in this case divide by the "register width" which for modbus is always 2
				startElement += maxByteRequest/2; 									   // I believe we want to in this case divide by the "register width" which for modbus is always 2 for holding regs
			} else {
				startElement += maxByteRequest*8;
			}
			thisRequest++;
		}
	}

	self.clearWritePacketTimeouts();
	self.writePacketArray = [];

//	outputLog("RLL is " + requestList.length);


	// Before we initialize the writePacketArray, we need to loop through all of them and clear timeouts.
	// The packetizer...

	while (requestNumber < requestList.length) {
		// Set up the read packet
		// Yes this is the same master sequence number shared with the read queue
		self.masterSequenceNumber += 1;
		if (self.masterSequenceNumber > 32767) {
			self.masterSequenceNumber = 1;
		}

		numItems = 0;

		self.writePacketArray.push(new PLCPacket());
		var thisPacketNumber = self.writePacketArray.length - 1;
		self.writePacketArray[thisPacketNumber].seqNum = self.masterSequenceNumber;
//		outputLog("Write Sequence Number is " + writePacketArray[thisPacketNumber].seqNum);

		self.writePacketArray[thisPacketNumber].itemList = [];  // Initialize as array.

		for (var i = requestNumber; i < requestList.length; i++) {

			if (numItems == 1) {
				break;  // Used to break when packet was full.  Now break when we can't fit this packet in here.
			}

			requestNumber++;
			numItems++;
			self.writePacketArray[thisPacketNumber].itemList.push(requestList[i]);
		}
	}
	outputLog("WPAL is " + self.writePacketArray.length, 1,self.connectionID);
}


NodeMBTCP.prototype.prepareReadPacket = function() {
	var self = this;
	var itemList = self.polledReadBlockList;				// The items are the actual items requested by the user
	var requestList = [];						// The request list consists of the block list, split into chunks readable by PDU.
	var startOfSlice, endOfSlice, oldEndCoil, demandEndCoil;

	// Validity check.
	for (i=itemList.length-1;i>=0;i--) {
		if (itemList[i] === undefined) {
			itemList.splice(i,1);
			outputLog("Dropping an undefined request item.",0,self.connectionID);
		}
	}

	// Sort the items using the sort function, by type and offset.
	itemList.sort(itemListSorter);

	// Just exit if there are no items.
	if (itemList.length == 0) {
		return undefined;
	}

	self.globalReadBlockList = [];

	// ...because you have to start your optimization somewhere.
	self.globalReadBlockList[0] = itemList[0];
	self.globalReadBlockList[0].itemReference = [];
	self.globalReadBlockList[0].itemReference.push(itemList[0]);

	var thisBlock = 0;
	itemList[0].block = thisBlock;
	var maxByteRequest = 248; // Could do 250 but that's not divisible by 4 for REAL
	// used to use 4*Math.floor((self.maxPDU - 18)/4);  // Absolutely must not break a real array into two requests.  Maybe we can extend by two bytes when not DINT/REAL/INT.



	// Optimize the items into blocks
	for (i=1;i<itemList.length;i++) {
		// Skip T, C, P types
		if ((itemList[i].areaMBCode !== self.globalReadBlockList[thisBlock].areaMBCode) ||   	// Can't optimize between areas
				(!self.isOptimizableArea(itemList[i].areaMBCode)) || 					// May as well try to optimize everything.
				((itemList[i].offset - self.globalReadBlockList[thisBlock].offset + itemList[i].byteLength) > maxByteRequest) ||      	// If this request puts us over our max byte length, create a new block for consistency reasons.
				((itemList[i].offset - (self.globalReadBlockList[thisBlock].offset + self.globalReadBlockList[thisBlock].byteLength) > self.maxGap) && itemList[i].startRegister > 20000) ||
				((itemList[i].offset - (self.globalReadBlockList[thisBlock].offset + self.globalReadBlockList[thisBlock].byteLength) > self.maxGap*8) && itemList[i].startRegister < 20000)) {		// If our gap is large, create a new block.
			// At this point we give up and create a new block.
			thisBlock = thisBlock + 1;
			self.globalReadBlockList[thisBlock] = itemList[i]; // By reference.
//				itemList[i].block = thisBlock; // Don't need to do this.
			self.globalReadBlockList[thisBlock].isOptimized = false;
			self.globalReadBlockList[thisBlock].itemReference = [];
			self.globalReadBlockList[thisBlock].itemReference.push(itemList[i]);
//			outputLog("Not optimizing.");
		} else {
			outputLog("Performing optimization of item " + itemList[i].addr + " with " + self.globalReadBlockList[thisBlock].addr,1,self.connectionID);
			// This next line checks the maximum.
			// Think of this situation - we have a large request of 40 bytes starting at byte 10.
			//	Then someone else wants one byte starting at byte 12.  The block length doesn't change.
			//
			// But if we had 40 bytes starting at byte 10 (which gives us byte 10-49) and we want byte 50, our byte length is 50-10 + 1 = 41.
//worked when complicated.			globalReadBlockList[thisBlock].byteLength = Math.max(globalReadBlockList[thisBlock].byteLength, ((itemList[i].offset - globalReadBlockList[thisBlock].offset) + Math.ceil(itemList[i].byteLength/itemList[i].multidtypelen))*itemList[i].multidtypelen);
//			console.log("THISBL", self.globalReadBlockList[thisBlock].byteLength);
//			console.log("THIS OFFSET", itemList[i].offset);
//			console.log("S.GRBL[tb].offset ", self.globalReadBlockList[thisBlock].offset);
//			console.log("math ceil", Math.ceil(itemList[i].byteLength/itemList[i].multidtypelen));
//			console.log("mdtl", itemList[i].multidtypelen);

			if (itemList[i].startRegister < 20000) { // Coils and inputs must be special-cased for Modbus
				self.globalReadBlockList[thisBlock].byteLength =
					Math.max(
						self.globalReadBlockList[thisBlock].byteLength,
						(Math.floor((itemList[i].offset - self.globalReadBlockList[thisBlock].offset)/8) + itemList[i].totalArrayLength)
					);
			} else {
				self.globalReadBlockList[thisBlock].byteLength =
					Math.max(
					self.globalReadBlockList[thisBlock].byteLength,
					((itemList[i].offset - self.globalReadBlockList[thisBlock].offset) + Math.ceil(itemList[i].byteLength/itemList[i].multidtypelen))*itemList[i].multidtypelen
//	trial that did not work				(itemList[i].offset - self.globalReadBlockList[thisBlock].offset)*2 + (Math.ceil(itemList[i].byteLength/itemList[i].multidtypelen))*itemList[i].multidtypelen
					);
			}
			outputLog("Optimized byte length is now " + self.globalReadBlockList[thisBlock].byteLength,1,self.connectionID);

//			globalReadBlockList[thisBlock].subelement = 0;  // We can't read just a timer preset, for example,

			// Point the buffers (byte and quality) to a sliced version of the optimized block.  This is by reference (same area of memory)
			if (itemList[i].startRegister < 20000) {  // Again a special case.
				startOfSlice = Math.floor((itemList[i].offset - self.globalReadBlockList[thisBlock].offset)/8);  // multidtype len is guaranteed to be 1

				// For Modbus coils and input bits, we need to modify this bit offset
				itemList[i].bitOffset = ((itemList[i].offset - self.globalReadBlockList[thisBlock].offset) % 8);

				// For Modbus coils and input bits, the array length is often different than the byte length and we need to keep track of it.
				oldEndCoil = self.globalReadBlockList[thisBlock].offset + self.globalReadBlockList[thisBlock].totalArrayLength - 1;
				demandEndCoil = itemList[i].offset + itemList[i].totalArrayLength - 1;
				if (demandEndCoil > oldEndCoil) {
					self.globalReadBlockList[thisBlock].totalArrayLength += (demandEndCoil - oldEndCoil);
				}
			} else {
				// Do we want to multiply by multidtypelen here?
				startOfSlice = (itemList[i].offset - self.globalReadBlockList[thisBlock].offset)*2; // NO, NO, NO - not the dtype length - start of slice varies with register width.  itemList[i].multidtypelen;
			}

//			console.log('ILI offset ' + itemList[i].offset + ' SGRBLTBO ' + self.globalReadBlockList[thisBlock].offset + 'IMDTL ' + itemList[i].multidtypelen);
			endOfSlice = startOfSlice + itemList[i].byteLength;
//			outputLog("SOS + EOS " + startOfSlice + " " + endOfSlice);
			itemList[i].byteBuffer = self.globalReadBlockList[thisBlock].byteBuffer.slice(startOfSlice, endOfSlice);
			itemList[i].qualityBuffer = self.globalReadBlockList[thisBlock].qualityBuffer.slice(startOfSlice, endOfSlice);

			// For now, change the request type here, and fill in some other things.

			// I am not sure we want to do these next two steps.
			// It seems like things get screwed up when we do this.
			// Since globalReadBlockList[thisBlock] exists already at this point, and our buffer is already set, let's not do this now.
			// globalReadBlockList[thisBlock].datatype = 'BYTE';
			// globalReadBlockList[thisBlock].dtypelen = 1;
			self.globalReadBlockList[thisBlock].isOptimized = true;
			self.globalReadBlockList[thisBlock].itemReference.push(itemList[i]);
		}
	}

	var thisRequest = 0;

//	outputLog("Preparing the read packet...");

	// Split the blocks into requests, if they're too large.
	for (i=0;i<self.globalReadBlockList.length;i++) {
		// Always create a request for a globalReadBlockList.
		requestList[thisRequest] = self.globalReadBlockList[i].clone();

		// How many parts?
		self.globalReadBlockList[i].parts = Math.ceil(self.globalReadBlockList[i].byteLength/maxByteRequest);
//		outputLog("globalReadBlockList " + i + " parts is " + globalReadBlockList[i].parts + " offset is " + globalReadBlockList[i].offset + " MBR is " + maxByteRequest);
		var startElement = self.globalReadBlockList[i].offset;
		var remainingLength = self.globalReadBlockList[i].byteLength;
		var remainingTotalArrayLength = self.globalReadBlockList[i].totalArrayLength;

		self.globalReadBlockList[i].requestReference = [];

		// If we need to spread the sending/receiving over multiple packets...
		for (j=0;j<self.globalReadBlockList[i].parts;j++) {
			requestList[thisRequest] = self.globalReadBlockList[i].clone();
			self.globalReadBlockList[i].requestReference.push(requestList[thisRequest]);
			//outputLog(globalReadBlockList[i]);
			//outputLog(globalReadBlockList.slice(i,i+1));
			requestList[thisRequest].offset = startElement;
			requestList[thisRequest].byteLength = Math.min(maxByteRequest,remainingLength);
			// Modbus needs the "total array length" for a number of bits.  Other protocols just read extra bits - not always safe with Modbus.
			requestList[thisRequest].totalArrayLength = Math.min(maxByteRequest*8,remainingLength*8,self.globalReadBlockList[i].totalArrayLength);
			requestList[thisRequest].byteLengthWithFill = requestList[thisRequest].byteLength;
			if (requestList[thisRequest].byteLengthWithFill % 2) { requestList[thisRequest].byteLengthWithFill += 1; };
			// Just for now...  I am not sure if we really want to do this in this case.
			if (self.globalReadBlockList[i].parts > 1) {
				requestList[thisRequest].datatype = 'BYTE';
				requestList[thisRequest].dtypelen = 1;
				requestList[thisRequest].arrayLength = requestList[thisRequest].byteLength;//globalReadBlockList[thisBlock].byteLength;
			}
			remainingLength -= maxByteRequest;
			if (self.globalReadBlockList[i].startRegister > 20000) {
//				startElement += maxByteRequest/requestList[thisRequest].multidtypelen;
				startElement += maxByteRequest/2; 									   // I believe we want to in this case divide by the "register width" which for modbus is always 2 for holding regs
			} else {
				startElement += maxByteRequest*8;
			}
			thisRequest++;
		}
	}

	//requestList[5].offset = 243;
	//	requestList = globalReadBlockList;

	// The packetizer...
	var requestNumber = 0;
	var itemsThisPacket;

	self.clearReadPacketTimeouts();
	self.readPacketArray = [];

//	outputLog("Request list length is " + requestList.length);

	while (requestNumber < requestList.length) {
		// Set up the read packet
		self.masterSequenceNumber += 1;
		if (self.masterSequenceNumber > 32767) {
			self.masterSequenceNumber = 1;
		}

		var numItems = 0;

		self.readPacketArray.push(new PLCPacket());
		var thisPacketNumber = self.readPacketArray.length - 1;
		self.readPacketArray[thisPacketNumber].seqNum = self.masterSequenceNumber;
//		outputLog("Sequence Number is " + self.readPacketArray[thisPacketNumber].seqNum);

		self.readPacketArray[thisPacketNumber].itemList = [];  // Initialize as array.

		for (var i = requestNumber; i < requestList.length; i++) {
			if (numItems >= 1) {
				break;  // We can't fit this packet in here.  For now, this is always the case with Modbus.
			}
			requestNumber++;
			numItems++;
			self.readPacketArray[thisPacketNumber].itemList.push(requestList[i]);
		}
	}
	self.readPacketValid = true;
}

NodeMBTCP.prototype.sendReadPacket = function() {
	var self = this;
	var i, j, curLength, returnedBfr, routerLength;
	var flagReconnect = false;

	outputLog("SendReadPacket called",1,self.connectionID);

	for (i = 0;i < self.readPacketArray.length; i++) {
		if (self.readPacketArray[i].sent) { continue; }
		if (self.parallelJobsNow >= self.maxParallel) { continue; }
		// From here down is SENDING the packet
		self.readPacketArray[i].reqTime = process.hrtime();

		curLength = 0;
		routerLength = 0;

		// For TCP we always need a Modbus header with the seq number, etc.
        if (!self.connectionParams.RTU) {
            self.Header.copy(self.readReq, curLength);
            curLength = self.Header.length;
        }

		// Write the sequence number to the offset in the Modbus packet.  Eventually this should be moved to within the FOR loop if we keep a FOR loop.
        if (!self.connectionParams.RTU) {
            self.readReq.writeUInt16BE(self.readPacketArray[i].seqNum, 0); // right at the start
        } else {
            self.lastSeqNum = i; // not really...  self.readPacketArray[i].seqNum;
        }

		// The FOR loop is left in here for now, but really we are only doing one request per packet for now.
		for (j = 0; j < self.readPacketArray[i].itemList.length; j++) {
			returnedBfr = MBAddrToBuffer(self.readPacketArray[i].itemList[j], false, self.connectionID);

			outputLog('The Returned Modbus Buffer is:',2,self.connectionID);
			outputLog(returnedBfr,2,self.connectionID);
			outputLog("The returned buffer length is " + returnedBfr.length,2,self.connectionID);

			returnedBfr.copy(self.readReq, curLength);
			curLength += returnedBfr.length;
		}

		// This is the overall message length for the Modbus request, minus most of the header, only for TCP
        if (!self.connectionParams.RTU) {
            self.readReq.writeUInt16BE(curLength - 6, 4);
        }

        // Add the CRC
        if (self.connectionParams.RTU) {
            self.readReq.writeUInt16LE(crc16(self.readReq.slice(0, curLength)), curLength);
            curLength += 2;
        }

		outputLog("The Returned buffer is:",2,self.connectionID);
		outputLog(returnedBfr,2,self.connectionID);

		if (self.isoConnectionState == 4) {
			self.readPacketArray[i].timeout = setTimeout(function(){
				self.packetTimeout.apply(self,arguments);
			}, self.globalTimeout, "read", self.readPacketArray[i].seqNum);
			self.isoclient.write(self.readReq.slice(0,curLength));  // was 31
			self.readPacketArray[i].sent = true;
			self.readPacketArray[i].rcvd = false;
			self.readPacketArray[i].timeoutError = false;
			self.parallelJobsNow += 1;
			outputLog('Sending Read Packet SEQ ' + self.readPacketArray[i].seqNum,1,self.connectionID);
		} else {
//			outputLog('Somehow got into read block without proper isoConnectionState of 4.  Disconnect.');
//			connectionReset();
//			setTimeout(connectNow, 2000, connectionParams);
// Note we aren't incrementing maxParallel so we are actually going to time out on all our packets all at once.
			self.readPacketArray[i].sent = true;
			self.readPacketArray[i].rcvd = false;
			self.readPacketArray[i].timeoutError = true;
			if (!flagReconnect) {
				// Prevent duplicates
				outputLog('Not Sending Read Packet because we are not connected - ISO CS is ' + self.isoConnectionState,0,self.connectionID);
			}
			// This is essentially an instantTimeout.
			if (self.isoConnectionState == 0) {
				flagReconnect = true;
			}
			outputLog('Requesting PacketTimeout Due to ISO CS NOT 4 - READ SN ' + self.readPacketArray[i].seqNum,1,self.connectionID);
			self.readPacketArray[i].timeout = setTimeout(function() {
				self.packetTimeout.apply(self, arguments);
			}, 0, "read", self.readPacketArray[i].seqNum);
		}
	}

	if (flagReconnect) {
//		console.log("Asking for callback next tick and my ID is " + self.connectionID);
		setTimeout(function() {
//			console.log("Next tick is here and my ID is " + self.connectionID);
			outputLog("The scheduled reconnect from sendReadPacket is happening now",1,self.connectionID);
			self.connectNow(self.connectionParams);  // We used to do this NOW - not NextTick() as we need to mark isoConnectionState as 1 right now.  Otherwise we queue up LOTS of connects and crash.
		}, 0);
	}
}

NodeMBTCP.prototype.sendWritePacket = function() {
	var self = this;
	var dataBuffer, itemDataBuffer, dataBufferPointer, curLength, returnedBfr, flagReconnect = false;
	dataBuffer = Buffer.alloc(8192);

	self.writeInQueue = false;

	for (i=0;i<self.writePacketArray.length;i++) {
		if (self.writePacketArray[i].sent) { continue; }
		if (self.parallelJobsNow >= self.maxParallel) { continue; }
		// From here down is SENDING the packet
		self.writePacketArray[i].reqTime = process.hrtime();

		curLength = 0;

        // For TCP we always need a Modbus header with the seq number, etc.
        if (!self.connectionParams.RTU) {
            self.Header.copy(self.readReq, curLength);
            curLength = self.Header.length;
        }

		// Write the sequence number to the offset in the Modbus packet.  Eventually this should be moved to within the FOR loop if we keep a FOR loop.
        if (!self.connectionParams.RTU) {
            self.writeReq.writeUInt16BE(self.writePacketArray[i].seqNum, 0);
        } else {
            self.lastSeqNum = self.writePacketArray[i].seqNum;
        }

		dataBufferPointer = 0;
		for (var j = 0; j < self.writePacketArray[i].itemList.length; j++) {
			returnedBfr = MBAddrToBuffer(self.writePacketArray[i].itemList[j], true, self.connectionID);

			outputLog(returnedBfr);
			returnedBfr.copy(self.writeReq, curLength);
			curLength += returnedBfr.length;
		}

        // Write the length to the header starting at the 5th byte if we're TCP
        if (!self.connectionParams.RTU) {
            self.writeReq.writeUInt16BE(curLength - 6, 4);
        }

        // Add the CRC
        if (self.connectionParams.RTU) {
            self.writeReq.writeUInt16LE(crc16(self.writeReq.slice(0, curLength)), curLength);
            curLength += 2;
        }

		outputLog("The returned buffer length is " + returnedBfr.length,1,self.connectionID);

		if (self.isoConnectionState === 4) {
			self.writePacketArray[i].timeout = setTimeout(function() {
				self.packetTimeout.apply(self, arguments);
			}, self.globalTimeout, "write", self.writePacketArray[i].seqNum);
//			console.log(self.writeReq.slice(0,curLength));
			self.isoclient.write(self.writeReq.slice(0,curLength));  // was 31
			self.writePacketArray[i].sent = true;
			self.writePacketArray[i].rcvd = false;
			self.writePacketArray[i].timeoutError = false;
			self.parallelJobsNow += 1;
			outputLog('Sending Write Packet With Sequence Number ' + self.writePacketArray[i].seqNum,1,self.connectionID);
		} else {
//			outputLog('Somehow got into write block without proper isoConnectionState of 4.  Disconnect.');
//			connectionReset();
//			setTimeout(connectNow, 2000, connectionParams);
			// This is essentially an instantTimeout.
			self.writePacketArray[i].sent = true;
			self.writePacketArray[i].rcvd = false;
			self.writePacketArray[i].timeoutError = true;

			// Without the scopePlaceholder, this doesn't work.   writePacketArray[i] becomes undefined.
			// The reason is that the value i is part of a closure and when seen "nextTick" has the same value
			// it would have just after the FOR loop is done.
			// (The FOR statement will increment it to beyond the array, then exit after the condition fails)
			// scopePlaceholder works as the array is de-referenced NOW, not "nextTick".
			var scopePlaceholder = self.writePacketArray[i].seqNum;
			process.nextTick(function() {
				self.packetTimeout("write", scopePlaceholder);
			});
			if (self.isoConnectionState == 0) {
				flagReconnect = true;
			}
		}
	}
	if (flagReconnect) {
//		console.log("Asking for callback next tick and my ID is " + self.connectionID);
		setTimeout(function() {
//			console.log("Next tick is here and my ID is " + self.connectionID);
			outputLog("The scheduled reconnect from sendWritePacket is happening now",1,self.connectionID);
			self.connectNow(self.connectionParams);  // We used to do this NOW - not NextTick() as we need to mark isoConnectionState as 1 right now.  Otherwise we queue up LOTS of connects and crash.
		}, 0);
	}
}

NodeMBTCP.prototype.isOptimizableArea = function(area) {
	var self = this;
	// for PCCC always say yes.
	if (self.doNotOptimize) { return false; } // Are we skipping all optimization due to user request?

	return true;
}

NodeMBTCP.prototype.onResponse = function(data) {
	var self = this;
	// Packet Validity Check.  Note that this will pass even with a "not available" response received from the server.
	// For length calculation and verification:
	// data[4] = COTP header length. Normally 2.  This doesn't include the length byte so add 1.
	// read(13) is parameter length.  Normally 4.
	// read(14) is data length.  (Includes item headers)
	// 12 is length of "S7 header"
	// Then we need to add 4 for TPKT header.

	// Decrement our parallel jobs now

	// NOT SO FAST - can't do this here.  If we time out, then later get the reply, we can't decrement this twice.  Or the CPU will not like us.  Do it if not rcvd.  parallelJobsNow--;

	outputLog(data,2,self.connectionID);  // Only log the entire buffer at high debug level
	outputLog("onResponse called with length " + data.length,1,self.connectionID);

    if ((!self.connectionParams.RTU) && data.length < 9) {
		outputLog('DATA LESS THAN 9 BYTES RECEIVED.  TOTAL CONNECTION RESET.',0,self.connectionID);
		outputLog(data,0,self.connectionID);
		self.connectionReset();
//		setTimeout(connectNow, 2000, connectionParams);
		return null;
	}

    if ((!self.connectionParams.RTU) && (data.length < (data.readInt16BE(4) + 6))) {
		outputLog('DATA LESS THAN MARKED LENGTH RECEIVED.  TOTAL CONNECTION RESET.',0,self.connectionID);
		outputLog(data,0,self.connectionID);
		self.connectionReset();
//		setTimeout(connectNow, 2000, connectionParams);
		return null;
	}

	// The smallest read packet will pass a length check of 25.  For a 1-item write response with no data, length will be 22.
    if ((!self.connectionParams.RTU) && (data.length > (data.readInt16BE(4) + 6))) {
		outputLog("An oversize packet was detected.  Excess length is " + (data.length - data.readInt16BE(4) - 6) + ".  ",0,self.connectionID);
		outputLog("Usually because two packets were sent at nearly the same time by the PLC.",0,self.connectionID);
		outputLog("We slice the buffer and schedule the second half for later processing.",0,self.connectionID);
//		setTimeout(onResponse, 0, data.slice(data.readInt16BE(2) + 24));  // This re-triggers this same function with the sliced-up buffer.
		process.nextTick(function(){
			self.onResponse(data.slice(data.readInt16BE(4) + 6))
		});  // This re-triggers this same function with the sliced-up buffer.
// was used as a test		setTimeout(process.exit, 2000);
	}

    if (self.connectionParams.RTU) {
        if (data.length < 6) {
            // Some functions may return less than this but looking at slave ID, function number, one word of data and CRC we should not see less than 6
            outputLog('DATA LESS THAN 6 BYTES RECEIVED.  TOTAL CONNECTION RESET.',0,self.connectionID);
            outputLog(data,0,self.connectionID);
            self.connectionReset();
            //		setTimeout(connectNow, 2000, connectionParams);
            return null;
        }

        var crcIn = data.readUInt16LE(data.length - 2);
        if (crcIn !== crc16(data.slice(0, -2))) {
            outputLog('CRC ERROR',0,self.connectionID);
            return null;
        }
    }


	outputLog('Valid Modbus Response Received (not yet checked for error)',1,self.connectionID);

	// Log the receive
	outputLog('Received ' + data.length + ' bytes of data from PLC.',1,self.connectionID);
	outputLog(data,2,self.connectionID);

	// Check the sequence number
	var foundSeqNum = undefined; // readPacketArray.length - 1;
	var packetCount = undefined;
	var isReadResponse = false, isWriteResponse = false;

    if (!self.connectionParams.RTU) {
	    outputLog("On Response - Sequence " + data.readUInt16BE(0),1,self.connectionID);

	    foundSeqNum = self.findReadIndexOfSeqNum(data.readUInt16BE(0));

    //	if (readPacketArray[packetCount] == undefined) {
	    if (foundSeqNum == undefined) {
		    foundSeqNum = self.findWriteIndexOfSeqNum(data.readUInt16BE(0));
		    if (foundSeqNum != undefined) {
    //		for (packetCount = 0; packetCount < writePacketArray.length; packetCount++) {
    //			if (writePacketArray[packetCount].seqNum == data.readUInt16BE(11)) {
    //				foundSeqNum = packetCount;

				    isWriteResponse = true;
    //				break;
		    }
	    } else {
		    isReadResponse = true;
		    outputLog("Received Read Response - Array Offset Is " + foundSeqNum,1,self.connectionID);
	    }
    }

    if (self.connectionParams.RTU && self.isReading()) { // Could probably use this for TCP too
        isReadResponse = true;
        foundSeqNum = self.lastSeqNum;
    }

    if (self.connectionParams.RTU && self.isWriting()) { // Could probably use this for TCP too
        isWriteResponse = true;
        foundSeqNum = self.lastSeqNum;
    }

    if (isWriteResponse) {
        self.writeResponse(data, foundSeqNum);
    } else if (isReadResponse) {
        self.readResponse(data, foundSeqNum);
    }

	if ((!isReadResponse) && (!isWriteResponse)) {
		outputLog("Sequence number that arrived wasn't a write reply either - dropping",0,self.connectionID);
		outputLog(data,0,self.connectionID);
// 	I guess this isn't a showstopper, just ignore it.
//		connectionReset();
//		setTimeout(connectNow, 2000, connectionParams);
		return null;
	}
}

NodeMBTCP.prototype.findReadIndexOfSeqNum = function(seqNum) {
	var self = this;
	var packetCounter;
	for (packetCounter = 0; packetCounter < self.readPacketArray.length; packetCounter++) {
		if (self.readPacketArray[packetCounter].seqNum == seqNum) {
			return packetCounter;
		}
	}
	return undefined;
}

NodeMBTCP.prototype.findWriteIndexOfSeqNum = function(seqNum) {
	var self = this;
	var packetCounter;
	for (packetCounter = 0; packetCounter < self.writePacketArray.length; packetCounter++) {
		if (self.writePacketArray[packetCounter].seqNum == seqNum) {
			return packetCounter;
		}
	}
	return undefined;
}

NodeMBTCP.prototype.writeResponse = function(data, foundSeqNum) {
	var self = this;
	var dataPointer = 21,i,anyBadQualities;

	if (!self.writePacketArray[foundSeqNum].sent) {
		outputLog('WARNING: Received a write packet that was not marked as sent',0,self.connectionID);
		return null;
	}
	if (self.writePacketArray[foundSeqNum].rcvd) {
		outputLog('WARNING: Received a write packet that was already marked as received',0,self.connectionID);
		return null;
	}

	for (itemCount = 0; itemCount < self.writePacketArray[foundSeqNum].itemList.length; itemCount++) {
		dataPointer = processMBWriteItem(data, self.writePacketArray[foundSeqNum].itemList[itemCount], dataPointer,self.connectionID);
		if (!dataPointer) {
			outputLog('Stopping Processing Write Response Packet due to unrecoverable packet error',0,self.connectionID);
			break;
		}
	}

	// Make a note of the time it took the PLC to process the request.
	self.writePacketArray[foundSeqNum].reqTime = process.hrtime(self.writePacketArray[foundSeqNum].reqTime);
	outputLog('Time is ' + self.writePacketArray[foundSeqNum].reqTime[0] + ' seconds and ' + Math.round(self.writePacketArray[foundSeqNum].reqTime[1]*10/1e6)/10 + ' ms.',1,self.connectionID);

//	writePacketArray.splice(foundSeqNum, 1);
	if (!self.writePacketArray[foundSeqNum].rcvd) {
		self.writePacketArray[foundSeqNum].rcvd = true;
		self.parallelJobsNow--;
	}
	clearTimeout(self.writePacketArray[foundSeqNum].timeout);

	if (!self.writePacketArray.every(doneSending)) {
//			readPacketInterval = setTimeout(prepareReadPacket, 3000);
		self.sendWritePacket();
		outputLog("Sending again",1,self.connectionID);
	} else {
		for (i=0;i<self.writePacketArray.length;i++) {
			self.writePacketArray[i].sent = false;
			self.writePacketArray[i].rcvd = false;
		}

		anyBadQualities = false;

		for (i=0;i<self.globalWriteBlockList.length;i++) {
			// Post-process the write code and apply the quality.
			// Loop through the global block list...
			writePostProcess(self.globalWriteBlockList[i]);
			outputLog(self.globalWriteBlockList[i].addr + ' write completed with quality ' + self.globalWriteBlockList[i].writeQuality,0,self.connectionID);
			if (!isQualityOK(self.globalWriteBlockList[i].writeQuality)) { anyBadQualities = true; }
		}
		if (typeof(self.writeDoneCallback) === 'function') {
			self.writeDoneCallback(anyBadQualities);
		}
		if (self.resetPending) {
			self.resetNow();
		}
		if (self.isoConnectionState === 0) {
			self.connectNow(self.connectionParams, false);
		}
	}
}

NodeMBTCP.prototype.readResponse = function(data, foundSeqNum) {
	var self = this;
	var anyBadQualities,dataPointer = 21;  // For non-routed packets we start at byte 21 of the packet.  If we do routing it will be more than this.
	var dataObject = {};

	outputLog("ReadResponse called",1,self.connectionID);

	if (!self.readPacketArray[foundSeqNum].sent) {
		outputLog('WARNING: Received a read response packet that was not marked as sent',0,self.connectionID);
		//TODO - fix the network unreachable error that made us do this
		return null;
	}
	if (self.readPacketArray[foundSeqNum].rcvd) {
		outputLog('WARNING: Received a read response packet that was already marked as received',0,self.connectionID);
		return null;
	}

	for (itemCount = 0; itemCount < self.readPacketArray[foundSeqNum].itemList.length; itemCount++) {
		dataPointer = processMBPacket(data, self.readPacketArray[foundSeqNum].itemList[itemCount], dataPointer, self.connectionParams.RTU, self.connectionID);
		if (!dataPointer && typeof(data) !== "undefined") {
			// Don't bother showing this message on timeout.
			outputLog('Received a ZERO RESPONSE Processing Read Packet due to unrecoverable packet error',0,self.connectionID);
//			break;  // We rely on this for our timeout now.
		}
	}

	// Make a note of the time it took the PLC to process the request.
	self.readPacketArray[foundSeqNum].reqTime = process.hrtime(self.readPacketArray[foundSeqNum].reqTime);
	outputLog('Read Time is ' + self.readPacketArray[foundSeqNum].reqTime[0] + ' seconds and ' + Math.round(self.readPacketArray[foundSeqNum].reqTime[1]*10/1e6)/10 + ' ms.',1,self.connectionID);

	// Do the bookkeeping for packet and timeout.
	if (!self.readPacketArray[foundSeqNum].rcvd) {
		self.readPacketArray[foundSeqNum].rcvd = true;
		self.parallelJobsNow--;
		if (self.parallelJobsNow < 0) { self.parallelJobsNow = 0; }
	}
	clearTimeout(self.readPacketArray[foundSeqNum].timeout);

	if(self.readPacketArray.every(doneSending)) {  // if sendReadPacket returns true we're all done.
		// Mark our packets unread for next time.
		outputLog('Every packet done sending',1,self.connectionID);
		for (i=0;i<self.readPacketArray.length;i++) {
			self.readPacketArray[i].sent = false;
			self.readPacketArray[i].rcvd = false;
		}

		anyBadQualities = false;

		// Loop through the global block list...
		for (var i=0;i<self.globalReadBlockList.length;i++) {
			var lengthOffset = 0;
			// For each block, we loop through all the requests.  Remember, for all but large arrays, there will only be one.
			for (var j=0;j<self.globalReadBlockList[i].requestReference.length;j++) {
				// Now that our request is complete, we reassemble the BLOCK byte buffer as a copy of each and every request byte buffer.
				self.globalReadBlockList[i].requestReference[j].byteBuffer.copy(self.globalReadBlockList[i].byteBuffer,lengthOffset,0,self.globalReadBlockList[i].requestReference[j].byteLength);
				self.globalReadBlockList[i].requestReference[j].qualityBuffer.copy(self.globalReadBlockList[i].qualityBuffer,lengthOffset,0,self.globalReadBlockList[i].requestReference[j].byteLength);
				lengthOffset += self.globalReadBlockList[i].requestReference[j].byteLength;
			}
			// For each ITEM reference pointed to by the block, we process the item.
			for (var k=0;k<self.globalReadBlockList[i].itemReference.length;k++) {
//				outputLog(self.globalReadBlockList[i].itemReference[k].byteBuffer);
				processMBReadItem(self.globalReadBlockList[i].itemReference[k], self.connectionParams.RTU, self.connectionID);
				outputLog('Address ' + self.globalReadBlockList[i].itemReference[k].addr + ' has value ' + self.globalReadBlockList[i].itemReference[k].value + ' and quality ' + self.globalReadBlockList[i].itemReference[k].quality,1,self.connectionID);
				if (!isQualityOK(self.globalReadBlockList[i].itemReference[k].quality)) {
					anyBadQualities = true;
					dataObject[self.globalReadBlockList[i].itemReference[k].useraddr] = self.globalReadBlockList[i].itemReference[k].quality;
				} else {
					dataObject[self.globalReadBlockList[i].itemReference[k].useraddr] = self.globalReadBlockList[i].itemReference[k].value;
				}
			}
		}

		// Inform our user that we are done and that the values are ready for pickup.

		outputLog("We are calling back our readDoneCallback.",1,self.connectionID);
		if (typeof(self.readDoneCallback === 'function')) {
			self.readDoneCallback(anyBadQualities, dataObject);
		}
		if (!self.writeInQueue) {
			if (self.resetPending) {
				self.resetNow();
			}
			if (self.isoConnectionState === 0) {
				self.connectNow(self.connectionParams, false);
			}
		}
		if (!self.isReading() && self.writeInQueue) {
			outputLog("SendWritePacket called because write was queued.",0,self.connectionID);
			self.sendWritePacket();
		}
	} else {
		outputLog("Calling SRP from RR",1,self.connectionID);
		self.sendReadPacket();
	}
}

NodeMBTCP.prototype.onClientDisconnect = function() {
	var self = this;
	outputLog('MODBUS/TCP DISCONNECTED.',0,self.connectionID);
	self.connectionCleanup();
	self.tryingToConnectNow = false;
}

NodeMBTCP.prototype.onClientClose = function() {
	var self = this;
    // clean up the connection now the socket has closed
//	self.connectionCleanup();
	self.connectionReset();  // We do a reset instead of a close now, as if close is somehow initiated at the other end we want to allow pending read/write to time out.
    // initiate the callback stored by dropConnection
    if (self.dropConnectionCallback) {
        self.dropConnectionCallback();
        // prevent any possiblity of the callback being called twice
        self.dropConnectionCallback = null;
        // and cancel the timeout
        clearTimeout(self.dropConnectionTimer);
    }
}

NodeMBTCP.prototype.connectionReset = function() {
	var self = this;
	self.isoConnectionState = 0;
	self.resetPending = true;
	outputLog('ConnectionReset is happening',0,self.connectionID);
	if (!self.isReading() && !self.isWriting() && !self.writeInQueue && typeof(self.resetTimeout) === 'undefined') {
		self.resetTimeout = setTimeout(function() {
			outputLog('Timed reset has happened. Ideally this would never be called as reset should be completed when done r/w.',0,self.connectionID);
			self.resetNow.apply(self, arguments);
		} ,4500);
	}
	// For now we wait until read() is called again to re-connect.
}

NodeMBTCP.prototype.resetNow = function() {
	var self = this;
	self.isoConnectionState = 0;
	self.isoclient.end();
	self.isoclient.destroy();
	outputLog('ResetNOW is happening',0,self.connectionID);
	self.resetPending = false;
	// In some cases, we can have a timeout scheduled for a reset, but we don't want to call it again in that case.
	// We only want to call a reset just as we are returning values.  Otherwise, we will get asked to read // more values and we will "break our promise" to always return something when asked.
	if (typeof(self.resetTimeout) !== 'undefined') {
		clearTimeout(self.resetTimeout);
		self.resetTimeout = undefined;
		outputLog('Clearing an earlier scheduled reset',0,self.connectionID);
	}
}

NodeMBTCP.prototype.connectionCleanup = function() {
	var self = this;
	self.isoConnectionState = 0;
	outputLog('Connection cleanup is happening',0,self.connectionID);
	if (typeof(self.isoclient) !== "undefined") {
		self.isoclient.destroy();
		self.isoclient.removeAllListeners('data');
		self.isoclient.removeAllListeners('error');
		self.isoclient.removeAllListeners('connect');
		self.isoclient.removeAllListeners('end');
		self.isoclient.removeAllListeners('close');
		self.isoclient.on('error',function() {
			outputLog('TCP socket error following connection cleanup',0,self.connectionID);
		});
	}
	clearTimeout(self.connectTimeout);
	clearTimeout(self.PDUTimeout);
	self.clearReadPacketTimeouts();  // Note this clears timeouts.
	self.clearWritePacketTimeouts();  // Note this clears timeouts.
}

function outputLog(txt, debugLevel, id) {
	var idtext;
	if (typeof(id) === 'undefined') {
		idtext = '';
	} else {
		idtext = ' ' + id;
	}
	if (typeof(debugLevel) === 'undefined' || effectiveDebugLevel >= debugLevel) { console.log('[' + process.hrtime() + idtext + '] ' + util.format(txt)); }
}

function doneSending(element) {
	return ((element.sent && element.rcvd) ? true : false);
}

function processMBPacket(theData, theItem, thePointer, RTU, theCID) {
	var remainingLength, headerAndPreamble;

	if (typeof(theData) === "undefined") {
		remainingLength = 0;
//		outputLog("Processing an undefined packet, likely due to timeout error");
	} else {
		remainingLength = theData.length;
	}

	var prePointer = thePointer;

	// Create a new buffer for the quality.
	theItem.qualityBuffer = Buffer.alloc(theItem.byteLength);
	theItem.qualityBuffer.fill(0xFF);  // Fill with 0xFF (255) which means NO QUALITY in the OPC world.

    if ((!(RTU) && (remainingLength < 9)) || (RTU && (remainingLength < 6)))  {
		theItem.valid = false;
		if (typeof(theData) !== "undefined") {
			theItem.errCode = 'Malformed Modbus Packet - Less Than Minimum Bytes.  TDL' + theData.length + 'TP' + thePointer + 'RL' + remainingLength;
			outputLog(theItem.errCode,0,theCID);
		} else {
			theItem.errCode = "Timeout error - zero length packet";
			outputLog(theItem.errCode,1,theCID);
		}
		return 0;   			// Hard to increment the pointer so we call it a malformed packet and we're done.
	}

    if (!(RTU) && (theData[2] !== 0x00 || theData[3] !== 0x00)) {
		theItem.valid = false;
		theItem.errCode = 'Invalid Modbus - Expected [2] to be 0x00 and [3] to be 0x00- got ' + theData[2] + ' and ' + theData[3];
		outputLog(theItem.errCode,0,theCID);
		return 1; //thePointer + reportedDataLength + 4;
	}

    if (!(RTU) && (theData[7] > 0x7f)) {
		theItem.valid = false;
		theItem.errCode = 'Modbus Error Response - Function ' + theData[7] + ' (' + (theData[7] - 128) + ') and code ' + theData[8];
		outputLog(theItem.errCode,0,theCID);
		return 1; //thePointer + reportedDataLength + 4;
    }

    if ((RTU) && (theData[2] > 0x7f)) {
        theItem.valid = false;
        theItem.errCode = 'Modbus Error Response - Function ' + theData[2] + ' (' + (theData[2] - 128) + ') and code ' + theData[3];
        outputLog(theItem.errCode,0,theCID);
        return 1; //thePointer + reportedDataLength + 4;
    }

	// There is no reported data length to check here -
	// reportedDataLength = theData[9];

    expectedLength = theItem.byteLength;

    if (!(RTU)) {
        headerAndPreamble = 9;
    } else {
        headerAndPreamble = 5;  // Slave, FC, length and CRC = 5 for reads at least
    }

	if (theData.length - headerAndPreamble !== expectedLength) {
		theItem.valid = false;
        theItem.errCode = 'Invalid Response Length - Expected ' + expectedLength + ' but got ' + (theData.length - headerAndPreamble) + ' bytes.';
		outputLog(theItem.errCode,0,theCID);
		return 1;
	}

    if (!(RTU) && (theData.length - 6 !== theData.readUInt16BE(4))) {  // Already checked before getting here but check again to be sure.
		theItem.valid = false;
		theItem.errCode = 'Invalid Response Length From Header - Expected ' + theData.readUInt16BE(4) + ' but got ' + (theData.length - 8) + ' bytes.';
		outputLog(theItem.errCode,0,theCID);
		return 1;
	}

	// Looks good so far.
	// Increment our data pointer past the status code, transport code and 2 byte length.
	thePointer += 4;

	var arrayIndex = 0;

    theItem.valid = true;
    if (!RTU) {
        theItem.byteBuffer = theData.slice(9); // This means take to end.
    } else {
        theItem.byteBuffer = theData.slice(3,theData.length-2); // This means take to end.
    }

	outputLog('Byte Buffer is:',2,theCID);
	outputLog(theItem.byteBuffer,2,theCID);

	theItem.qualityBuffer.fill(0xC0);  // Fill with 0xC0 (192) which means GOOD QUALITY in the OPC world.
	outputLog('Marking quality as good L' + theItem.qualityBuffer.length,2,theCID);

//	thePointer += theItem.byteLength; //WithFill;

//	if (((thePointer - prePointer) % 2)) { // Odd number.  With the S7 protocol we only request an even number of bytes.  So there will // be a filler byte.
//		thePointer += 1;
//	}

//	outputLog("We have an item value of " + theItem.value + " for " + theItem.addr + " and pointer of " + thePointer);

	return -1; //thePointer;
}

function processMBWriteItem(theData, theItem, thePointer,theCID) {

//	var remainingLength = theData.length - thePointer;  // Say if length is 39 and pointer is 35 we can access 35,36,37,38 = 4 bytes.
//	var prePointer = thePointer;

	if (typeof(theData) === 'undefined' || theData.length < 9) {  // Should be at least 11 bytes with 7 byte header
		theItem.valid = false;
		theItem.errCode = 'Malformed Reply Modbus Packet - Less Than 9 Bytes or Malformed Header.  ' + theData;
		outputLog(theItem.errCode,0,theCID);
		return 0;   			// Hard to increment the pointer so we call it a malformed packet and we're done.
	}

	var writeResponse = theData.readUInt8(7);

	if (writeResponse > 0x7f) {
		outputLog ('Received response ' + theData[7] + ' (' + (theData[7] - 128) + ') indicating write error of ' + theData[8] + ' on ' + theItem.addr,0,theCID);
		theItem.writeQualityBuffer.fill(0xFF);  // Note that ff is good in the S7 world but BAD in our fill here.
	} else {
		theItem.writeQualityBuffer.fill(0xC0);
	}

	return -1;
}

function writePostProcess(theItem) {
	var thePointer = 0;
	if (theItem.arrayLength === 1) {
		if (theItem.writeQualityBuffer[0] === 0xFF) {
			theItem.writeQuality = 'BAD';
		} else {
			theItem.writeQuality = 'OK';
		}
	} else {
		// Array value.
		theItem.writeQuality = [];
		for (arrayIndex = 0; arrayIndex < theItem.arrayLength; arrayIndex++) {
			if (theItem.writeQualityBuffer[thePointer] === 0xFF) {
				theItem.writeQuality[arrayIndex] = 'BAD';
			} else {
				theItem.writeQuality[arrayIndex] = 'OK';
			}
			if (theItem.datatype == 'X' ) {
				// For bit arrays, we have to do some tricky math to get the pointer to equal the byte offset.
				// Note that we add the bit offset here for the rare case of an array starting at other than zero.  We either have to
				// drop support for this at the request level or support it here.

				if ((((arrayIndex + theItem.bitOffset + 1) % 8) == 0) || (arrayIndex == theItem.arrayLength - 1)){
					thePointer += theItem.dtypelen;
					}
			} else {
				// Add to the pointer every time.
				thePointer += theItem.dtypelen;
			}
		}
	}
}


function processMBReadItem(theItem, RTU, theCID) {

	var thePointer = 0,tempBuffer = Buffer.alloc(4);

	if (theItem.arrayLength > 1) {
		// Array value.
		if (theItem.datatype != 'C' && theItem.datatype != 'CHAR') {
			theItem.value = [];
			theItem.quality = [];
		} else {
			theItem.value = '';
			theItem.quality = '';
		}
		var bitShiftAmount = theItem.bitOffset;
		for (arrayIndex = 0; arrayIndex < theItem.arrayLength; arrayIndex++) {
			if (theItem.qualityBuffer[thePointer] !== 0xC0) {
				theItem.value.push(theItem.badValue());
				theItem.quality.push('BAD ' + theItem.qualityBuffer[thePointer]);
			} else {
				// If we're a string, quality is not an array.
				if (theItem.quality instanceof Array) {
					theItem.quality.push('OK');
				} else {
					theItem.quality = 'OK';
				}
				switch(theItem.datatype) {

				case "REAL":
//					theItem.value.push(theItem.byteBuffer.readFloatBE(thePointer));
					theItem.value.push(getFloat(theItem.byteBuffer,thePointer, false));
					break;
				case "DWORD":
//					theItem.value.push(theItem.byteBuffer.readUInt32BE(thePointer));
					theItem.value.push(getUInt32(theItem.byteBuffer,thePointer, false));
					break;
				case "DINT":
//					theItem.value.push(theItem.byteBuffer.readInt32BE(thePointer));
					theItem.value.push(getInt32(theItem.byteBuffer,thePointer, false));
					break;
				case "WSREAL":
//					theItem.value.push(theItem.byteBuffer.readFloatBE(thePointer));
					theItem.value.push(getFloat(theItem.byteBuffer,thePointer, true));
					break;
				case "WSDWORD":
//					theItem.value.push(theItem.byteBuffer.readUInt32BE(thePointer));
					theItem.value.push(getUInt32(theItem.byteBuffer,thePointer, true));
					break;
				case "WSDINT":
//					theItem.value.push(theItem.byteBuffer.readInt32BE(thePointer));
					theItem.value.push(getInt32(theItem.byteBuffer,thePointer, true));
					break;
                case "INT":
                    theItem.value.push(theItem.byteBuffer.readInt16BE(thePointer));
					break;
				case "WORD":
                    theItem.value.push(theItem.byteBuffer.readUInt16BE(thePointer));
					break;
                case "X":
                    if (theItem.startRegister > 19999) {
                        theItem.value.push(((theItem.byteBuffer.readUInt16BE(thePointer) >> (bitShiftAmount)) & 1) ? true : false);
                    } else {
                        theItem.value.push(((theItem.byteBuffer.readUInt8(thePointer) >> (bitShiftAmount)) & 1) ? true : false);
                    }
				break;
				case "B":
				case "BYTE":
					theItem.value.push(theItem.byteBuffer.readUInt8(thePointer));
					break;

				case "C":
				case "CHAR":
					// Convert to string.
					theItem.value += String.fromCharCode(theItem.byteBuffer.readUInt8(thePointer));
					break;
				case "TIMER":
                case "COUNTER":
                    theItem.value.push(theItem.byteBuffer.readInt16BE(thePointer));
					break;

				default:
					outputLog("Unknown data type in response - should never happen.  Should have been caught earlier.  " + theItem.datatype,0,theCID);
					return 0;
				}
			}
			if (theItem.datatype == 'X' ) {
				// For bit arrays, we have to do some tricky math to get the pointer to equal the byte offset.
				// Note that we add the bit offset here for the rare case of an array starting at other than zero.  We either have to
				// drop support for this at the request level or support it here.
				bitShiftAmount++;
				if (theItem.startRegister > 19999) {
					if ((((arrayIndex + theItem.bitOffset + 1) % 16) == 0) || (arrayIndex == theItem.arrayLength - 1)){
						thePointer += theItem.dtypelen;
						bitShiftAmount = 0;
					}
				} else {
					if ((((arrayIndex + theItem.bitOffset + 1) % 8) == 0) || (arrayIndex == theItem.arrayLength - 1)){
						thePointer += theItem.dtypelen; // I guess this is 1 for bits.
						bitShiftAmount = 0;
					}
				}
			} else {
				// Add to the pointer every time.
				thePointer += theItem.dtypelen;
			}
		}
	} else {
		// Single value.
		if (theItem.qualityBuffer[thePointer] !== 0xC0) {
			theItem.value = theItem.badValue();
			theItem.quality = ('BAD ' + theItem.qualityBuffer[thePointer]);
			outputLog("Item Quality is Bad", 1, theCID);
		} else {
			theItem.quality = ('OK');
			outputLog("Item Datatype (single value) is " + theItem.datatype + " and Byte Offset is " + theItem.byteOffset, 1, theCID);
			switch(theItem.datatype) {

			case "REAL":
//				theItem.value = theItem.byteBuffer.readFloatBE(thePointer);
				theItem.value = getFloat(theItem.byteBuffer,thePointer, false);
				break;
			case "DWORD":
//				theItem.value = theItem.byteBuffer.readUInt32BE(thePointer);
				theItem.value = getUInt32(theItem.byteBuffer,thePointer, false);
				break;
			case "DINT":
//				theItem.value = theItem.byteBuffer.readInt32BE(thePointer);
				theItem.value = getInt32(theItem.byteBuffer,thePointer, false);
				break;
			case "WSREAL":
//				theItem.value = theItem.byteBuffer.readFloatBE(thePointer);
				theItem.value = getFloat(theItem.byteBuffer,thePointer, true);
				break;
			case "WSDWORD":
//				theItem.value = theItem.byteBuffer.readUInt32BE(thePointer);
				theItem.value = getUInt32(theItem.byteBuffer,thePointer, true);
				break;
			case "WSDINT":
//				theItem.value = theItem.byteBuffer.readInt32BE(thePointer);
				theItem.value = getInt32(theItem.byteBuffer,thePointer, true);
				break;
            case "INT":
                theItem.value = theItem.byteBuffer.readInt16BE(thePointer, false);
			    break;
            case "WORD":
                theItem.value = theItem.byteBuffer.readUInt16BE(thePointer, false);
			    break;
			case "X":
//			outputLog("Reading single Value ByteBufferLength is " + theItem.byteBuffer.length, 1);
                if (theItem.startRegister > 19999) {
                    theItem.value = (((theItem.byteBuffer.readUInt16BE(thePointer) >> (theItem.bitOffset)) & 1) ? true : false);
				} else {
					theItem.value = (((theItem.byteBuffer.readUInt8(thePointer) >> (theItem.bitOffset)) & 1) ? true : false);
				}
				break;
			case "B":
			case "BYTE":
				// No support as of yet for signed 8 bit.  This isn't that common in Siemens.
				theItem.value = theItem.byteBuffer.readUInt8(thePointer);
				break;
			case "C":
			case "CHAR":
				// No support as of yet for signed 8 bit.  This isn't that common in Siemens.
				theItem.value = String.fromCharCode(theItem.byteBuffer.readUInt8(thePointer));
				break;
			case "TIMER":
			case "COUNTER":
				theItem.value = theItem.byteBuffer.readInt16BE(thePointer + theItem.byteOffset);
				break;
			default:
				outputLog("Unknown data type in response - should never happen.  Should have been caught earlier.  " + theItem.datatype, theCID);
				return 0;
			}
		}
		thePointer += theItem.dtypelen;
	}

	if (((thePointer) % 2)) { // Odd number.  With the S7 protocol we only request an even number of bytes.  So there will be a filler byte.
		thePointer += 1;
	}

//	outputLog("We have an item value of " + theItem.value + " for " + theItem.addr + " and pointer of " + thePointer);
	return thePointer; // Should maybe return a value now???
}

function bufferizeMBItem(theItem, theCID) {
	var thePointer, theByte;
	theByte = 0;
	thePointer = 0; // After length and header

	if (theItem.arrayLength > 1) {
		// Array value.
		var bitShiftAmount = theItem.bitOffset;
		for (arrayIndex = 0; arrayIndex < theItem.arrayLength; arrayIndex++) {
			switch(theItem.datatype) {
				case "REAL":
					setFloat(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], false);
//					theItem.writeBuffer.writeFloatBE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "DWORD":
					setUInt32(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], false);
//					theItem.writeBuffer.writeInt32BE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "DINT":
					setInt32(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], false);
//					theItem.writeBuffer.writeInt32BE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "WSREAL":
					setFloat(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], true);
//					theItem.writeBuffer.writeFloatBE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "WSDWORD":
					setUInt32(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], true);
//					theItem.writeBuffer.writeInt32BE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "WSDINT":
					setInt32(theItem.writeBuffer, thePointer, theItem.writeValue[arrayIndex], true);
//					theItem.writeBuffer.writeInt32BE(theItem.writeValue[arrayIndex], thePointer);
					break;
				case "INT":
					theItem.writeBuffer.writeInt16BE(Math.round(theItem.writeValue[arrayIndex]), thePointer);
					break;
				case "WORD":
					theItem.writeBuffer.writeUInt16BE(Math.round(theItem.writeValue[arrayIndex]), thePointer);
					break;
				case "X":
					theByte = theByte | (((theItem.writeValue[arrayIndex] === true) ? 1 : 0) << bitShiftAmount);
					// Maybe not so efficient to do this every time when we only need to do it every 8.  Need to be careful with optimizations here for odd requests.
					theItem.writeBuffer.writeUInt8(theByte, thePointer);
					bitShiftAmount++;
					break;
				case "B":
				case "BYTE":
					theItem.writeBuffer.writeUInt8(Math.round(theItem.writeValue[arrayIndex]), thePointer);
					break;
				case "C":
				case "CHAR":
					// Convert to string.
//??					theItem.writeBuffer.writeUInt8(theItem.writeValue.toCharCode(), thePointer);
					theItem.writeBuffer.writeUInt8(theItem.writeValue.charCodeAt(arrayIndex), thePointer);
					break;
				case "TIMER":
				case "COUNTER":
					// I didn't think we supported arrays of timers and counters.
					theItem.writeBuffer.writeInt16BE(theItem.writeValue[arrayIndex], thePointer);
					break;
				default:
					outputLog("Unknown data type when preparing array write packet - should never happen.  Should have been caught earlier.  " + theItem.datatype, theCID);
					return 0;
			}
			if (theItem.datatype == 'X' ) {
				// For bit arrays, we have to do some tricky math to get the pointer to equal the byte offset.
				// Note that we add the bit offset here for the rare case of an array starting at other than zero.  We either have to
				// drop support for this at the request level or support it here.

				if ((((arrayIndex + theItem.bitOffset + 1) % 8) == 0) || (arrayIndex == theItem.arrayLength - 1)){
					thePointer += theItem.dtypelen;
					theByte = 0;
					bitShiftAmount = 0;
					}
			} else {
				// Add to the pointer every time.
				thePointer += theItem.dtypelen;
			}
		}
	} else {
		// Single value.
		switch(theItem.datatype) {

			case "REAL":
				setFloat(theItem.writeBuffer, thePointer, theItem.writeValue, false);
//				theItem.writeBuffer.writeFloatBE(theItem.writeValue, thePointer);
				break;
			case "DWORD":
				setUInt32(theItem.writeBuffer, thePointer, theItem.writeValue, false);
//				theItem.writeBuffer.writeUInt32BE(theItem.writeValue, thePointer);
				break;
			case "DINT":
				setInt32(theItem.writeBuffer, thePointer, theItem.writeValue, false);
//				theItem.writeBuffer.writeInt32BE(theItem.writeValue, thePointer);
				break;
			case "WSREAL":
				setFloat(theItem.writeBuffer, thePointer, theItem.writeValue, true);
//				theItem.writeBuffer.writeFloatBE(theItem.writeValue, thePointer);
				break;
			case "WSDWORD":
				setUInt32(theItem.writeBuffer, thePointer, theItem.writeValue, true);
//				theItem.writeBuffer.writeUInt32BE(theItem.writeValue, thePointer);
				break;
			case "WSDINT":
				setInt32(theItem.writeBuffer, thePointer, theItem.writeValue, true);
//				theItem.writeBuffer.writeInt32BE(theItem.writeValue, thePointer);
				break;
			case "INT":
				theItem.writeBuffer.writeInt16BE(Math.round(theItem.writeValue), thePointer);
				break;
			case "WORD":
				theItem.writeBuffer.writeUInt16BE(Math.round(theItem.writeValue), thePointer);
				break;
			case "X":
				theItem.writeBuffer.writeUInt8(((theItem.writeValue) ? 1 : 0), thePointer);  // checked ===true but this caused problems if you write 1
				outputLog("Datatype is X writing " + theItem.writeValue + " tpi " + theItem.writeBuffer[0],1,theCID);

// not here				theItem.writeBuffer[1] = 1; // Set transport code to "BIT" to write a single bit.
// not here				theItem.writeBuffer.writeUInt16BE(1, 2); // Write only one bit.
				break;
			case "B":
			case "BYTE":
				// No support as of yet for signed 8 bit.  This isn't that common in Siemens.
				theItem.writeBuffer.writeUInt8(Math.round(theItem.writeValue), thePointer);
				break;
			case "C":
			case "CHAR":
				// No support as of yet for signed 8 bit.  This isn't that common in Siemens.
				theItem.writeBuffer.writeUInt8(String.toCharCode(theItem.writeValue), thePointer);
				break;
			case "TIMER":
			case "COUNTER":
				theItem.writeBuffer.writeInt16BE(theItem.writeValue, thePointer);
				break;
			default:
				outputLog("Unknown data type in write prepare - should never happen.  Should have been caught earlier.  " + theItem.datatype,theCID);
				return 0;
		}
		thePointer += theItem.dtypelen;
	}
	return undefined;
}

function isQualityOK(obj) {
	if (typeof obj === "string") {
		if (obj !== 'OK') { return false; }
	} else if (_.isArray(obj)) {
		for (i = 0; i < obj.length; i++) {
			if (typeof obj[i] !== "string" || obj[i] !== 'OK') { return false; }
		}
	}
	return true;
}

function MBAddrToBuffer(addrinfo, isWriting, theCID) { // SLCAddrToBufferA2
	var writeLength, MBCommand = Buffer.alloc(300);  // 12 is max length with all fields at max.

	writeLength = isWriting ? (addrinfo.byteLength + 1) : 0; // Shouldn't ever be zero

	MBCommand[0] = addrinfo.slaveID;

	switch(addrinfo.subtractor) {
	case 1:
		MBCommand[1] = isWriting ? 15 : 1; // Even when writing only one coil we use FC15
		break;
	case 10001:
	case 100001:
		MBCommand[1] = 2; // Can't write
		break;
	case 30001:
	case 300001:
		MBCommand[1] = 4; // Can't write
		break;
	case 40001:
	case 400001:
		MBCommand[1] = isWriting ? 16 : 3;
		break;
	}

	if (addrinfo.subtractor == 30001 || addrinfo.subtractor == 40001 || addrinfo.subtractor == 300001 || addrinfo.subtractor == 400001) {
		MBCommand.writeUInt16BE(addrinfo.offset, 2);
		MBCommand.writeUInt16BE(addrinfo.byteLength/2, 4);
	}

	if (addrinfo.subtractor == 1 || addrinfo.subtractor == 10001 || addrinfo.subtractor == 100001) {
		MBCommand.writeUInt16BE(addrinfo.offset, 2);
		MBCommand.writeUInt16BE(addrinfo.totalArrayLength, 4);  // Really don't want to use byte length here - we can effectiveively request a fraction of a byte and sometimes need to.
	}

	if (isWriting) {
		MBCommand[6] = writeLength - 1;
		addrinfo.writeBuffer.copy(MBCommand,7,0,writeLength - 1);
	}

	return MBCommand.slice(0,6+writeLength); // WriteLength is the length we write.  writeLength - 1 is the data length.
}

function stringToMBAddr(addr, useraddr, defaultID, theCID) {
	// Modbus format overview
	// Easier done by example
	// MYTAG=1 = 0x00001 or very first coil
	// MYTAG=10001 = first discrete input
	// MYTAG=10001,20 = array of 20 coils
	// MYTAG=30001:INT = regular input register
	// MYTAG=40001.6 = bit in word (supported read only for now)
	// , indicates array.  MYTAG=40001:INT,10SLAVE1 is an array of 1-10
	"use strict";
	var theItem, splitString, splitString2, splitStringSlave, prefix, postDotAlpha, postDotNumeric, forceBitDtype, startRegisterString;
	theItem = new PLCItem();

	splitStringSlave = addr.split('SLAVE');
	// splitStringSlave[0] = address part
	// splitStringSlave[1] = slave
	if (splitStringSlave.length > 1) {
		theItem.slaveID = parseInt(splitStringSlave[1].replace(/[A-z]/gi, ''), 10);
	} else {
		theItem.slaveID = defaultID;
	}

	// First check to see if we are looking for an array.
	splitString2 = splitStringSlave[0].split(',');
	// splitString2[0] = address part
	// splitString2[1] = array length

	if (splitString2.length == 2) {
		theItem.arrayLength = parseInt(splitString2[1].replace(/[A-z]/gi, ''), 10);
		//if (typeof(theItem.arrayLength) === 'undefined') { theItem.arrayLength = 1; }
	} else {
		theItem.arrayLength = 1;
	}

	theItem.totalArrayLength = theItem.arrayLength;

	// Then take the first part (minus the array specifier if it exists)
	splitString = splitString2[0].split(':');
	// splitString[0] = start register with possible bit reference
	// splitString[1] = Datatype, if specified.  It may not be specified.

	startRegisterString = splitString[0].split('.');
	// startRegisterString[0] = start register
	// startRegisterString[1] = bit reference, if it exists

//	outputLog('StartRegisterString[0] is ' + startRegisterString[0],2);

	// Convert to integer
	theItem.startRegister = parseInt(startRegisterString[0].replace(/[A-z]/gi, ''),10);

	if (startRegisterString[0].search("RD") >= 0) {
		theItem.startRegister += 40001;
		if (splitString.length == 1) {
			splitString.push("DINT");
		}
	} else if (startRegisterString[0].search("R") >= 0) {
		theItem.startRegister += 40001;
		if (splitString.length == 1) {
			splitString.push("INT");
		}
	}

	// Check the bit offset
	if (startRegisterString.length > 2) {
		outputLog("Error - You can only specify one bit offset",0,theCID);
		return undefined;
	}

	// Do some checks on the startRegister to make sure it's sane.
	if (theItem.startRegister <= 0 || theItem.startRegister == 10000 || theItem.startRegister == 100000 || (theItem.startRegister > 19999 && theItem.startRegister < 30001) || theItem.startRegister == 40000 || theItem.startRegister > 465535 || (theItem.startRegister > 365535 && theItem.startRegister < 400001)) {
		outputLog("Error - Starting register  must be 1-9999, 10001-19999, 30001-39999, 40001-49999, 300001-365535, 400001-465535",0,theCID);
		return undefined;
	}

	// We'll store this now, process it later to check for validity.

	if (splitString.length > 1) {
		theItem.datatype = splitString[1];
	} else {
		theItem.datatype = undefined;
	}

	//console.log('SRS');
	//console.log(startRegisterString);
	//console.log(theItem.datatype);

	// Process the bit offset
	if (startRegisterString.length == 2) {
		theItem.bitOffset = parseInt(startRegisterString[1],10);
	} else {
		theItem.bitOffset = undefined;
	}

	// Figure out our code and subtractor
	if (theItem.startRegister < 10000) {
		theItem.areaMBCode = 0;
		theItem.subtractor = 1;
		theItem.datatype = "X";
		theItem.dtypelen = 1;  // in bytes - maybe should be 2 for 4000x registers.
	} else if (theItem.startRegister < 20000) {
		theItem.areaMBCode = 1;
		theItem.subtractor = 10001;
		theItem.datatype = "X";
		theItem.dtypelen = 1;  // in bytes - maybe should be 2 for 4000x registers.
	} else if (theItem.startRegister < 40000) {
		theItem.areaMBCode = 3;
		theItem.subtractor = 30001;
		if (typeof(theItem.datatype) === 'undefined') {
			theItem.datatype = typeof(theItem.bitOffset) === 'undefined' ? "INT" : "X";
		}
	} else if (theItem.startRegister < 50000) {
		theItem.areaMBCode = 4;
		theItem.subtractor = 40001;
		if (typeof(theItem.datatype) === 'undefined') {
			theItem.datatype = typeof(theItem.bitOffset) === 'undefined' ? "INT" : "X";
		}
	} else if (theItem.startRegister < 400000) {
		theItem.areaMBCode = 3;
		theItem.subtractor = 300001;
		if (typeof(theItem.datatype) === 'undefined') {
			theItem.datatype = typeof(theItem.bitOffset) === 'undefined' ? "INT" : "X";
		}
	} else if (theItem.startRegister < 500000) {
		theItem.areaMBCode = 4;
		theItem.subtractor = 400001;
		if (typeof(theItem.datatype) === 'undefined') {
			theItem.datatype = typeof(theItem.bitOffset) === 'undefined' ? "INT" : "X";
		}
	} else {
		outputLog("Error - Register Not Valid",0,theCID);
		return undefined;
	}

	if (theItem.subtractor !== 30001 && theItem.subtractor !== 40001 && theItem.subtractor !== 300001 && theItem.subtractor !== 400001 && typeof(theItem.bitOffset) !== 'undefined') {
		outputLog("Error - You can only specify bit offset with 3000x and 4000x registers",0,theCID);
		return undefined;
	}


	// Calculate our offset.  Note this an integer offset for 3000x/4000x and a bit offset otherwise.
	theItem.offset = theItem.startRegister - theItem.subtractor;

//	outputLog('Start register is ' + theItem.startRegister + ' subtractor is ' + theItem.subtractor,2);

	if (theItem.startRegister > 30000) {
		switch (theItem.datatype) {
		case "X":
//			theItem.dtypelen = 1;  // in bytes - maybe should be 2 for 4000x registers.
//			break;
		case "INT":
		case "WORD":
			theItem.dtypelen = 2;  // in bytes - note that even for type 'X' on 4000x registers we are 2.
			break;
		case "REAL":
		case "DINT":
		case "DWORD":
		case "WSREAL":
		case "WSDINT":
		case "WSDWORD":
			theItem.dtypelen = 4;  // in bytes
			break;
		default:
			outputLog("Unknown data type entered - " + theItem.datatype, theCID);
			return undefined;
		}
	}

	theItem.multidtypelen = theItem.dtypelen;  // in bytes

	// Save the address from the argument for later use and reference
	theItem.addr = addr;
	if (useraddr === undefined) {
		theItem.useraddr = addr;
	} else {
		theItem.useraddr = useraddr;
	}

	if (theItem.datatype === 'X') {
		if (typeof(theItem.bitOffset) === 'undefined') {
			theItem.bitOffset = 0;
		}
		theItem.byteLength = Math.ceil((theItem.bitOffset + theItem.arrayLength) / 8);
		if (theItem.byteLength % 2 && theItem.startRegister > 19999) { theItem.byteLength += 1; }  // This shouldn't get triggered, really.
	} else {
		theItem.byteLength = theItem.arrayLength * theItem.dtypelen;
	}

//	outputLog(' Arr length is ' + theItem.arrayLength + ' and DTL is ' + theItem.dtypelen);
//	outputLog(' PCCC Code is ' + decimalToHexString(theItem.areaMBCode) + ' and addrtype is ' + theItem.addrtype);
//	outputLog(' Offset is ' + decimalToHexString(theItem.offset) + ' and bit offset is ' + theItem.bitOffset);
//	outputLog(' Byte length is ' + theItem.byteLength);

	theItem.byteLengthWithFill = theItem.byteLength;
	if (theItem.startRegister > 19999 && (theItem.byteLengthWithFill % 2)) { theItem.byteLengthWithFill += 1; }  // S7 will add a filler byte.  Use this expected reply length for PDU calculations.

	return theItem;
}

function outputError(txt) {
	util.error(txt);
}

function decimalToHexString(number)
{
    if (number < 0)
    {
    	number = 0xFFFFFFFF + number + 1;
    }

    return "0x" + number.toString(16).toUpperCase();
}

function PLCPacket() {
	this.seqNum = undefined;				// Made-up sequence number to watch for.
	this.itemList = undefined;  			// This will be assigned the object that details what was in the request.
	this.reqTime = undefined;
	this.sent = false;						// Have we sent the packet yet?
	this.rcvd = false;						// Are we waiting on a reply?
	this.timeoutError = undefined;			// The packet is marked with error on timeout so we don't then later switch to good data.
	this.timeout = undefined;				// The timeout for use with clearTimeout()
}

function PLCItem() { // Object
	// MB only
	this.areaMBCode = undefined;
	this.startRegister = undefined;
	this.subtractor = undefined;
	this.slaveID = undefined;

	// Save the original address
	this.addr = undefined;
	this.useraddr = undefined;

	// First group is properties to do with S7 - these alone define the address.
	this.addrtype = undefined;
	this.datatype = undefined;
	this.dbNumber = undefined;
	this.bitOffset = undefined;
	this.byteOffset = undefined;
	this.offset = undefined;
	this.arrayLength = undefined;
	this.totalArrayLength = undefined; // Includes optimizations "tacked on" the end for Modbus coils.

	// These next properties can be calculated from the above properties, and may be converted to functions.
	this.dtypelen = undefined;
	this.multidtypelen = undefined; // multi-datatype length.  Different than dtypelen when requesting a timer preset, for example, which has width two but dtypelen of 2.
	this.areaS7Code = undefined;
	this.byteLength = undefined;
	this.byteLengthWithFill = undefined;

	// Note that read transport codes and write transport codes will be the same except for bits which are read as bytes but written as bits
	this.readTransportCode = undefined;
	this.writeTransportCode = undefined;

	// This is where the data can go that arrives in the packet, before calculating the value.
	this.byteBuffer = Buffer.alloc(8192);
	this.writeBuffer = Buffer.alloc(8192);

	// We use the "quality buffer" to keep track of whether or not the requests were successful.
	// Otherwise, it is too easy to lose track of arrays that may only be partially complete.
	this.qualityBuffer = Buffer.alloc(8192);
	this.writeQualityBuffer = Buffer.alloc(8192);

	// Then we have item properties
	this.value = undefined;
	this.writeValue = undefined;
	this.valid = false;
	this.errCode = undefined;

	// Then we have result properties
	this.part = undefined;
	this.maxPart = undefined;

	// Block properties
	this.isOptimized = false;
	this.resultReference = undefined;
	this.itemReference = undefined;

	// And functions...
	this.clone = function() {
		var newObj = new PLCItem();
		for (var i in this) {
			if (i == 'clone') continue;
			newObj[i] = this[i];
		} return newObj;
	};

	// Bad value function definition
	this.badValue = function() {
		switch (this.datatype){
		case "REAL":
		case "WSREAL":
			return 0.0;
		case "DWORD":
		case "DINT":
		case "WSDWORD":
		case "WSDINT":
		case "INT":
		case "WORD":
		case "B":
		case "BYTE":
		case "TIMER":
		case "COUNTER":
			return 0;
		case "X":
			return false;
		case "C":
		case "CHAR":
			// Convert to string.
			return "";
		default:
			outputLog("Unknown data type when figuring out bad value - should never happen.  Should have been caught earlier.  " + this.datatype);
			return 0;
		}
	};
}

function itemListSorter(a, b) {
	// Feel free to manipulate these next two lines...
	if (a.areaMBCode < b.areaMBCode) { return -1; }
	if (a.areaMBCode > b.areaMBCode) { return 1; }

	// But for byte offset we need to start at 0.
	if (a.offset < b.offset) { return -1; }
	if (a.offset > b.offset) { return 1; }

	// Then bit offset
	if (a.bitOffset < b.bitOffset) { return -1; }
	if (a.bitOffset > b.bitOffset) { return 1; }

	// Then item length - most first.  This way smaller items are optimized into bigger ones if they have the same starting value.
	if (a.byteLength > b.byteLength) { return -1; }
	if (a.byteLength < b.byteLength) { return 1; }
}

function doNothing(arg) {
	return arg;
}

function getFloat(buf, ptr, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf[0] = buf[ptr+2];
		newBuf[1] = buf[ptr+3];
		newBuf[2] = buf[ptr+0];
		newBuf[3] = buf[ptr+1];
		return newBuf.readFloatBE(0);
	} else {
		return buf.readFloatBE(ptr);
	}
}

function setFloat(buf, ptr, val, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf.writeFloatBE(val, 0);
		buf[ptr+2] = newBuf[0];
		buf[ptr+3] = newBuf[1];
		buf[ptr+0] = newBuf[2];
		buf[ptr+1] = newBuf[3];
		return;
	} else {
		buf.writeFloatBE(val, ptr);
		return;
	}
}

function getInt32(buf, ptr, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf[0] = buf[ptr+2];
		newBuf[1] = buf[ptr+3];
		newBuf[2] = buf[ptr+0];
		newBuf[3] = buf[ptr+1];
		return newBuf.readInt32BE(0);
	} else {
		return buf.readInt32BE(ptr);
	}
}

function setInt32(buf, ptr, val, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf.writeInt32BE(Math.round(val), 0);
		buf[ptr+2] = newBuf[0];
		buf[ptr+3] = newBuf[1];
		buf[ptr+0] = newBuf[2];
		buf[ptr+1] = newBuf[3];
		return;
	} else {
		buf.writeInt32BE(val, ptr);
		return;
	}
}


function getUInt32(buf, ptr, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf[0] = buf[ptr+2];
		newBuf[1] = buf[ptr+3];
		newBuf[2] = buf[ptr+0];
		newBuf[3] = buf[ptr+1];
		return newBuf.readUInt32BE(0);
	} else {
		return buf.readUInt32BE(ptr);
	}
}

function setUInt32(buf, ptr, val, swap) {
	var newBuf = Buffer.alloc(4);
	if (swap) {
		newBuf.writeUInt32BE(Math.round(val), 0);
		buf[ptr+2] = newBuf[0];
		buf[ptr+3] = newBuf[1];
		buf[ptr+0] = newBuf[2];
		buf[ptr+1] = newBuf[3];
		return;
	} else {
		buf.writeUInt32BE(val, ptr);
		return;
	}
}


