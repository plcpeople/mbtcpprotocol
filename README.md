MBTCPPROTOCOL
=============

MBTCPPROTOCOL is a library that allows communication to PLCs and controllers using MODBUS/TCP or MODBUS RTU over TCP.  

It supports (in beta) a Modbus slave connected with a non-Modbus serial/Ethernet converter.  Use the RTU option.

There is not yet support for double-precision floating point (64 bit) but this could be added with an appropriate test platform.

IMPORTANT: You usually must specify the slave ID as part of the tag when the default of 1 is not good enough.

IMPORTANT: The default ID up to and including version 0.1.12 is 255.  The default ID after that is 1, to mimic the old behavior, set defaultID: 255 in the connection parameters.

Tag format supports the following:
	TAG1=00001SLAVE2		// Leading zeros optional, 1-based "coils".  Read/write.  Always writes with "force multiple coils" FC15, never uses the single.  
	TAG1=00001,100   		// Array of coils from slave 1 (formerly 255).  Read/write.  
	TAG1=10001   			// Input status.  Read-only.  
	TAG1=10001,100  		// Input status array.  Read-only.  
	TAG1=30001   			// Input register.  Read-only.  
	TAG1=30001,100  		// Input register array.  Read-only.  
	TAG1=30001:REAL,100 	// Input register array as REAL.  Read-only.  Assumes skip registers, like first is 30001, next is 30003, etc.  
	TAG1=30001:WSREAL,100 	// Input register array as word-swapped REAL.  Read-only.  Assumes skip registers, like first is 30001, next is 30003, etc.  
	TAG1=30001:DINT			// Input register as DINT
	TAG1=30001:WSDINT		// Input register as word-swapped DINT
	TAG1=30001:DWORD		// Input register as unsigned DINT
	TAG1=30001:WSDWORD		// Input register as word-swapped unsigned DINT
	TAG1=30001.0,15			// First Input register split into bits (read-only)
	TAG1=40001   			// Holding register.  Read-only.  
	TAG1=40001,100  		// Holding register array.  Read-only.  
	TAG1=40001:REAL,100 	// Holding register array as REAL.  Assumes skip registers, like first is 40001, next is 40003, etc.  
	TAG1=40001:WSREAL,100 	// Holding register array as word-swapped REAL.  Assumes skip registers, like first is 40001, next is 40003, etc.  
	TAG1=40001:DINTSLAVE2	// Holding register as DINT, from slave 1
	TAG1=40001:WSDINTSLAVE2	// Holding register as word-swapped DINT, from slave 1
	TAG1=40001:DWORD		// Holding register as unsigned DINT
	TAG1=40001:WSDWORD		// Holding register as word-swapped unsigned DINT
	TAG1=40001.3			// Holding register bit (IMPORTANT NOTE: read-only - can't write individual bits of 4000x registers)
	TAG1=RD90				// Holding register 40091 as double integer
	TAG1=R190				// Holding register 40191 as integer


Installation:
	
	npm install mbtcpprotocol

Example usage:

	var NodeMBTCP = require('mbtcpprotocol');
	var conn = new NodeMBTCP();
	var doneReading = false;
	var doneWriting = false;

	conn.initiateConnection({port: 502, host: '192.168.8.199', defaultID: 1, RTU: false}, connected); // defaultID defaults to 1 and RTU defaults to false but shown here for illustration

	function connected(err) {
		if (typeof(err) !== "undefined") {
			// We have an error.  Maybe the PLC is not reachable.  
			console.log(err);
			process.exit();
		}
		conn.setTranslationCB(tagLookup);
		conn.addItems(['TEST1']);
	//	conn.addItems(['TEST1', 'TEST4']);
	//	conn.removeItems(['TEST2', 'TEST3']);  // Demo.  
	//	conn.writeItems(['TEST5', 'TEST6'], [ 867.5309, 9 ], valuesWritten);  // You can write an array of items.  
	//	conn.writeItems('TEST7', [ 666, 777 ], valuesWritten);  // You can write a single array item too.  
		conn.readAllItems(valuesReady);	
	}

	function valuesReady(anythingBad) {
		if (anythingBad) { console.log("SOMETHING WENT WRONG READING VALUES!!!!"); }
		console.log("Value is " + conn.findItem('TEST1').value + " quality is " + conn.findItem('TEST1').quality);
		doneReading = true;
		if (doneWriting) { process.exit(); }
	}

	function valuesWritten(anythingBad) {
		if (anythingBad) { console.log("SOMETHING WENT WRONG WRITING VALUES!!!!"); }
		console.log("Done writing.");
		doneWriting = true;
		if (doneReading) { process.exit(); }
	}

	// This is a very simple "tag lookup" callback function that would eventually be replaced with either a database findOne(), or a large array in memory.  
	// Note that the return value is a controller absolute address and datatype specifier.  
	// If you want to use absolute addresses only, you can do that too.  
	function tagLookup(tag) {
		switch (tag) {
	case 'TEST1':
		return "030SLAVE1";
		case 'TEST2':
			return '40100.0SLAVE1';
		case 'TEST3':
			return '40101.1SLAVE1';	
		case 'TEST4':
			return '30001.1SLAVE1';		
		case 'TEST5':
			return '00008SLAVE1';		
		case 'TEST6':
			return '00003,20SLAVE1';
		case 'TEST7':
			return '00001,3000SLAVE1';
		case 'TEST8':
			return '40001:DINTSLAVE1';
		case 'TEST9':
			return '40001:DINT,400SLAVE1';
		case 'TEST10':
			return '30001.0,16SLAVE1';	
		default:
			return undefined;
		}
	}
