const async = require('async');
const { S3Client, CopyObjectCommand, DeleteObjectCommand, DeleteBucketCommand, ListObjectVersionsCommand, PutBucketTaggingCommand, PutBucketEncryptionCommand, PutBucketVersioningCommand } = require('@aws-sdk/client-s3');

let self = module.exports = {
	/**
	 * Lists all the files found in an S3 bucket - including versions and delete markers
	 * @param {*} bucketName The bucket to check - this can be v1 or v2
	 * @param {*} folder An internal folder or file to limit file listing by. Set this to '' to see all bucket contents
	 * @param {*} credentials The S3 credentials 
	 * @return {*} Returns an object that contains all versioned files and delete markers found in the specified location
	 */
	listFileVersions(bucketName, folder, credentials) {
		return new Promise((resolve, reject) => { 
			getClient(credentials).then(async s3 => {
				// Declare truncated as a flag that the while loop is based on.
				let truncated = true;
				// Declare a variable to which the key of the last element is assigned to in the response.
				let pageMarker;
				// Declare an empty array that stores our results
				let returnObject = {Versions: [], DeleteMarkers: []};
				// Declare command parameters
				let bucketParams = { Bucket: bucketName, Prefix: folder };
	
				if('bucket' in credentials){
					bucketParams = { Bucket: credentials['bucket'], Prefix: bucketName + '/' + folder };
				}
	
				// While loop that runs until 'response.truncated' is false.
				while (truncated) {
					try {
						// List all objects in the bucket
						const response = await s3.send(new ListObjectVersionsCommand(bucketParams));
	
						// Add each found item to the return array
						if(Object.prototype.hasOwnProperty.call(response, 'Versions')){
							returnObject.Versions = returnObject.Versions.concat(response.Versions);
						}
	
						// Add each delete marker to the return array
						if(Object.prototype.hasOwnProperty.call(response, 'DeleteMarkers')){
							returnObject.DeleteMarkers = returnObject.DeleteMarkers.concat(response.DeleteMarkers);
						}
	
						// Log the key of every item in the response to standard output.
						truncated = response.IsTruncated;
						// If truncated is true, assign the key of the last element in the response to the pageMarker variable.
						if (truncated) {
							pageMarker = response.Versions.slice(-1)[0].Key;
							// Assign the pageMarker value to bucketParams so that the next iteration starts from the new pageMarker.
							bucketParams.KeyMarker = pageMarker;
						}
						// At end of the list, response.truncated is false, and the function exits the while loop.
					} catch (err) {
						truncated = false;
						reject(err);
					}
				}
				// Return the filled object array
				resolve(returnObject);
			});
		});
	}, 

	/**
	 * Lists all the backups found in a bucket
	 * @param {*} bucketName The bucket to retrieve backups from - this can be v1 or v2
	 * @param {*} credentials The S3 credentials 
	 * @return {*} Returns an array that holds each folder name found in the /backups directory in the bucket
	 */
	listBackups(bucketName, credentials) {
		return new Promise((resolve, reject) => { 
			getClient(credentials).then(async s3 => {
				// Declare truncated as a flag that the while loop is based on.
				let truncated = true;
				// Declare a variable to which the key of the last element is assigned to in the response.
				let pageMarker;
				// Declare an array that stores our results
				let backupsArray = [];
				// Declare command parameters
				let bucketParams = { Bucket: bucketName, Prefix: 'backups/', Delimiter: '/' };
	
				if('bucket' in credentials){
					bucketParams = { Bucket: credentials['bucket'], Prefix: bucketName+'/backups/', Delimiter: '/'};
				}
	
				// While loop that runs until 'response.truncated' is false.
				while (truncated) {
					try {
						// List all backup folders in the bucket
						const response = await s3.send(new ListObjectVersionsCommand(bucketParams));
	
						// Add each found item to the return array
						if(Object.prototype.hasOwnProperty.call(response, 'CommonPrefixes')){
							let prefixes = response.CommonPrefixes;
							let splitIndex = 1;
							if('bucket' in credentials){
								splitIndex = 2;
							}
							for(let curItem of prefixes){
								let backupName = curItem.Prefix.split('/')[splitIndex];
								backupsArray.push(backupName);
							}
						}
	
						// Log the key of every item in the response to standard output.
						truncated = response.IsTruncated;
						// If truncated is true, assign the key of the last element in the response to the pageMarker variable.
						if (truncated) {
							pageMarker = response.CommonPrefixes.slice(-1)[0].Key;
							// Assign the pageMarker value to bucketParams so that the next iteration starts from the new pageMarker.
							bucketParams.KeyMarker = pageMarker;
						}
						// At end of the list, response.truncated is false, and the function exits the while loop.
					} catch (err) {
						truncated = false;
						reject(err);
					}
				}
				// Return the filled array
				resolve(backupsArray);
			});
		});
	},

	/**
	 * Extracts the date from an S3 bucket name
	 * Example: 20220502230023-47180780406793064490 -> 05/02/2022 OR 03242022132649 -> 03/24/2022
	 * @param {*} backupName The backup folder name to parse
	 * @returns Returns a date object if the date could be extracted
	 */
	calculateDate(backupName) {
		return new Promise((resolve, reject) => { 
			try {
				// Split the bucket name into parts, and separate the date section into year, month, and date
				let year;
				let month;
				let day;

				// Parse the date based on backup naming system
				if(backupName.length === 14){
					month = parseInt(backupName.slice(0, 2)) - 1; //Months are in 0-11 format
					day = backupName.slice(2, 4); 
					year = backupName.slice(4, 8);
				} else if (backupName.length === 35){
					year = backupName.slice(0, 4);
					month = parseInt(backupName.slice(4, 6)) - 1; //Months are in 0-11 format
					day = backupName.slice(6, 8);
				} else {
					reject('calculateDate Error: Invalid backup name length for backup ' + backupName);
				}
		
				// Return the date object
				let date = new Date(year, month, day);

				// If the date is not valid, reject
				if(isNaN(date)) {
					throw new Error('Invalid date');
				}

				// Resolve the date
				resolve(date);	
			} catch (err) {
				reject(err);
			}
		});
	},

	/**
	 * Deletes a version-protected object from S3
	 * @param {*} bucketName The bucket that contains the object
	 * @param {*} filePath The path to the object to be deleted
	 * @param {*} versionId The object's version ID
	 * @param {*} credentials The S3 credentials 
	 * @returns Returns a message when the object is sucessfully deleted
	 */
	deleteObjectVersion(bucketName, filePath, versionId, credentials) {
		return new Promise((resolve, reject) => { 
			try{
				let deleteParams = { Bucket: bucketName, Key: filePath, VersionId: versionId };

				// Check if the bucket is a V2 folder, and if the credentials are valid
				if('bucket' in credentials){
					if (typeof credentials.bucket === 'string' && credentials.bucket.length > 0) {
						deleteParams = { Bucket: credentials['bucket'], Key: filePath, VersionId: versionId };
					} else {
						return reject('Invalid bucket credentials recieved while deleting object ' + deleteParams.Bucket + '/' + deleteParams.Key);
					}
				}

				// Create the client and delete the object
				getClient(credentials)
					.then(s3 => {
						return s3.send(new DeleteObjectCommand(deleteParams));
					})
					.then(() => {
						resolve('Deleted ' + deleteParams.Bucket + '/' + deleteParams.Key);
					})
					.catch(error => {
						reject(error);
					});
			} catch (err) {
				reject(err);
			}
		});
	},

	/**
	 * Deletes all files in a version-protected bucket or folder. If a bucket is specified, it also deletes the bucket
	 * @param {*} bucketName The bucket name from which to delete files
	 * @param {*} folder Optionally, a folder to delete all files from. If this value is '', the entire bucket is deleted
	 * @param {*} credentials The S3 credentials
	 * @returns Returns a message when all objects are sucessfully deleted
	 */
	deleteAll(bucketName, folder, credentials) {
		return new Promise((resolve, reject) => { 
			try{
				getClient(credentials).then(s3 => {
					this.listFileVersions(bucketName, folder, credentials).then(async files => {
						// Initialize variables
						let items = [];
						let failedItems = [];
						let itemsDeletedCount = 0;
		
						//Create an async await queue that creates creates a delete object promise for each item it processes
						let itemQueue = async.queue((task, done) => {
							this.deleteObjectVersion(task.bucketName, task.path, task.versionId, task.credentials).then(() => {
								// Increment the count of items successfully deleted
								itemsDeletedCount++;
								done();
							}).catch(error => {
								// Add the item to the error output array
								failedItems.push(JSON.stringify({'bucket': task.bucketName, 'path': task.path, 'version': task.versionId, error}));
								done();
							});
						}, 100);
	
						// Create the callback function that will trigger when the queue is empty
						itemQueue.drain(async () => {
							// Once all items have been deleted, check if the bucket itself needs to be deleted
							if(folder === '') {
								this.listFileVersions(bucketName, '', credentials).then(remainingFiles => {
									let remainingFileCount = remainingFiles.Versions.length + remainingFiles.DeleteMarkers.length;
									// Check if the bucket is currently empty and is a bucket instead of a V2 folder
									if((remainingFileCount === 0) && !('bucket' in credentials)) {
										s3.send(new DeleteBucketCommand({ Bucket: bucketName })).then(() => {
											resolve('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder + ' and deleted bucket successfully');
										}).catch(error => {
											reject('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder + ', but experienced error deleting bucket: ' + error);
										});
									} 
								}).catch(error => {
									reject(error);
								});
							}

							//Resolve the promise
							if(failedItems.length === 0){
								resolve('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder);
							} else {
								let error = new Error('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder + ', and failed to delete: ' + failedItems.substring(0, 10));
								error.failedItems = failedItems;
								reject(error);
							}
						});
	
						// For each file version, add it to the list of items to be deleted
						for (let curFile of files.Versions) {
							let path = curFile.Key;
							let versionId = curFile.VersionId;
							let task = {bucketName, path, versionId, credentials};
							items.push(task);
						}
		
						// For each delete marker, add it to the list of items to be deleted
						for (let curDeleteMarker of files.DeleteMarkers) {
							let path = curDeleteMarker.Key;
							let versionId = curDeleteMarker.VersionId;
							let task = {bucketName, path, versionId, credentials};
							items.push(task);
						}
	
						// Push all items to be deleted into the queue at once
						itemQueue.push(items);
		
					}).catch(error => {
						reject(error);
					});
				}).catch(error => {
					reject(error);
				});
			} catch (err) {
				reject(err);
			}
		});
	},

	/**
	 * Helper function that copies files in S3 between two locations
	 * @param {*} sourceInstallationId The source installation to copy files from
	 * @param {*} sourceCredentials The credentials of the source installation - important for determining v1/v2 location
	 * @param {*} destinationInstallationId The destination installation to copy files to
	 * @param {*} destinationCredentials The credentials of the destination installation - important for determining v1/v2 location
	 * @param {*} filesObject An object containing all public and private files that will be copied
	 * @returns If successful, resolves without output
	 */
	transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, destinationCredentials, filesObject) {
		return new Promise((resolve, reject) => {
			try{
				// Extracts all file objects and puts them into an array
				let publicVersions = filesObject['public']['Versions'];
				let privateVersions = filesObject['private']['Versions'];
				let allObjects = publicVersions.concat(privateVersions);

				// Extracts keys and region data from the credential object
				let { accessKeyId, secretAccessKey, region } = sourceCredentials;
				let clientParams = {
					credentials: {
						accessKeyId: accessKeyId,
						secretAccessKey: secretAccessKey
					},
					region: region
				}; 

				// Creates the S3 client object
				const client = new S3Client(clientParams);

				// Create a queue for processing copy commands, 25 at a time
				let copyCommands = [];
				let itemQueue = async.queue(function(task, done) {
					client.send(new CopyObjectCommand(task)).then(() => {
						done();
					}).catch(error => {
						console.log(error);
						done();
					});
				}, 25);

				// Resolve when the queue is empty
				itemQueue.drain(() => {
					return resolve();
				});

				// Process each object from the backup
				for(let currentObject of allObjects) {
					try {
						// Do not copy non-current files, as they may interfere with versioning
						if(currentObject.IsLatest === false){
							continue;
						}

						// Create the file's copy source
						let copysource;
						if (sourceCredentials.version && sourceCredentials.version >= 2) {
							copysource = sourceCredentials.bucket + '/' + sourceInstallationId + '/' + currentObject.Key + '?versionId=' + currentObject.VersionId;
						} else {
							copysource = sourceInstallationId + '/' + currentObject.Key + '?versionId=' + currentObject.VersionId;
						}

						// Create the file's destination bucket and key
						let bucket;
						let key;
						if (destinationCredentials.version && destinationCredentials.version >= 2) {
							bucket = destinationCredentials.bucket;
							key = destinationInstallationId + '/' + currentObject.Key;
						} else {
							bucket = destinationInstallationId;
							key = currentObject.Key;
						}

						// Create copy parameters and add it to the array that will be put into the processing queue
						let copyParameters = {CopySource: copysource, Bucket: bucket, Key: key};
						copyCommands.push(copyParameters);

					} catch (err) {
						console.log('Error retrieving copy data for ' + currentObject.Key, err);
					}
				}

				// Push all copy commands into the queue at once
				itemQueue.push(copyCommands);

			} catch(error) { 
				return reject(error);
			}
		});
	}
};

/**
 * Creates an S3 client object with the given credentials
 * @param {*} credentials The credentials to use to create an S3 client object
 * @returns An S3 client object 
 */
async function getClient(credentials) {
	try {
		// Extracts keys and region data from the credential object
		let { accessKeyId, secretAccessKey, region } = credentials;
		let clientParams = {
			credentials: {
				accessKeyId: accessKeyId,
				secretAccessKey: secretAccessKey
			},
			region: region
		}; 

		// Creates and returns the new S3 client object
		const client = new S3Client(clientParams);
		return client;
	} catch (err) {
		console.log('Error: ', err);
	}
}

/**
 * Applys tags to a bucket
 * @param {*} client
 * @param {string} bucket 
 * @param {Object[]} tags 
 * @param {number} retries
 * @returns 
 */
function putBucketTags(client, bucket, tags, retries) {
	retries = retries || 0;
	return new Promise((resolve, reject) => {

		//Need to set tags
		let params = {
			Bucket: bucket,
			Tagging: {
				TagSet: tags
			}
		};
		
		client.send(new PutBucketTaggingCommand(params))
			.then(resolve)
			.catch((error) => {
				if (retries < 5) {
					let retryTime = retries > 1 ? Math.pow(2, retries) + (Math.random() * 6 - 3) : Math.pow(2, retries);
					setTimeout(() => {
						putBucketTags(client, bucket, tags, retries + 1)
							.then(resolve)
							.catch(reject);
					}, retryTime);
				} else {
					reject(error);
				}
			});
	});
}

/**
 * Applys default encryption to a bucket
 * @param {*} client
 * @param {string} bucket 
 * @param {number} retries
 * @returns 
 */
function putBucketEncryption(client, bucket, retries) {
	retries = retries || 0;
	return new Promise((resolve, reject) => {

		//Need to set encryption
		let params = {
			Bucket: bucket,
			ServerSideEncryptionConfiguration: {
				Rules: [
					{ApplyServerSideEncryptionByDefault: {SSEAlgorithm: 'AES256'}}
				]
			}
		};
		client.send(new PutBucketEncryptionCommand(params))
			.then(resolve)
			.catch((error) => {
				if (retries < 5) {
					let retryTime = retries > 1 ? Math.pow(2, retries) + (Math.random() * 6 - 3) : Math.pow(2, retries);
					setTimeout(() => {
						putBucketEncryption(client, bucket, retries + 1)
							.then(resolve)
							.catch(reject);
					}, retryTime);
				} else {
					reject(error);
				}
			});
	});
}

/**
 * Applys versioning to a bucket
 * @param {*} client
 * @param {string} bucket 
 * @param {number} retries
 * @returns 
 */
function putBucketVersioning(client, bucket, retries) {
	retries = retries || 0;
	return new Promise((resolve, reject) => {

		//Need to set versioning
		let params = {
			Bucket: bucket,
			VersioningConfiguration: {
				Status: 'Enabled'
			}
		};
		client.send(new PutBucketVersioningCommand(params))
			.then(resolve)
			.catch((error) => {
				if (retries < 5) {
					let retryTime = retries > 1 ? Math.pow(2, retries) + (Math.random() * 6 - 3) : Math.pow(2, retries);
					setTimeout(() => {
						putBucketVersioning(client, bucket, retries + 1)
							.then(resolve)
							.catch(reject);
					}, retryTime);
				} else {
					reject(error);
				}
			});
	});
}