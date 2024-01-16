const async = require('async');
const { Storage } = require('@google-cloud/storage');
	
let self = module.exports = {
	/**
	 * Lists all files found in the GCP bucket for the installation - includes all file data, including versioning
	 * @param {*} installationId The installationID bucket to read files from
	 * @param {*} folder A specific filepath to limit search results by. Use '' to get all bucket contents
	 * @param {*} credentials The GCP credentials
	 * @return {*} Returns an object containing all GCP file information
	 */
	listFileVersions(installationId, folder, credentials) {
		return new Promise((resolve, reject) => {
			// Create the GCP client and bucket object
			let gcs = new Storage(credentials);
			let bucket = gcs.bucket(installationId);

			//Check if the bucket exists
			bucket.exists(async function (error, exists) {
				if (exists) {
					try {
						// Set the prefix if it is defined
						let bucketParams = {prefix: folder, versions: true};

						// Get files from bucket
						let [results] = await bucket.getFiles(bucketParams);

						// Return the file object
						resolve(results);
					} catch (error) {
						reject(new Error('listFileVersions Error: ' + error));
					}
				} else {
					// Check for an error and return appropriately
					if (error) {
						return reject(error);
					} else {
						return reject(new Error('listFileVersions Error: Bucket not found!'));
					}
				}
			});
		});
	},

	/**
	 * Lists all the backup folders found in the specified bucket
	 * @param {*} installationId The bucket to get backups from
	 * @param {*} credentials The GCP credentials
	 * @returns An array of string bucket names
	 */
	listBackups(installationId, credentials) {
		return new Promise((resolve, reject) => {
			// Create the GCP client and bucket object
			let gcs = new Storage(credentials);
			let bucket = gcs.bucket(installationId);

			//Check if the bucket exists
			bucket.exists(async function (error, exists) {
				if (exists) {
					try {
						// Ininitalize variables for recording backups
						let buckets = [];
						// List all files in the /backups folder in the bucket
						let options = {
							prefix: 'backups/'
						};
						let [files] = await gcs.bucket(installationId).getFiles(options);

						// For each file, extract the name of the backup
						files.forEach(file => {
							let filteredName = file.name;
							filteredName = filteredName.replace('backups/', '');
							filteredName = filteredName.split('/')[0];

							// If the backup name has not been found yet, record it 
							if(filteredName != '' && !buckets.includes(filteredName)){
								buckets.push(filteredName);
							}
						});

						// Return the array of backups
						resolve(buckets);
					} catch (error) {
						reject(new Error('listBackups Error: ' + error));
					}
				} else {
					if (error) {
						return reject(error);
					} else {
						return reject(new Error('listBackups Error: Bucket not found!'));
					}
				}
			});
		});
	},

	/**
	 * Extracts the date from a GCP bucket name
	 * Example: company1-i8667373ad74d4a27bcfa3545c6b63-2022-2-8_5_0_16-86887099031208640856 -> 02/08/2022
	 * @param {*} backupName The backup name to parse
	 * @returns Returns a date object if the date could be extracted
	 */
	calculateDate(backupName) {
		return new Promise((resolve, reject) => { 
			try {
				// Split the bucket name into parts, and separate the date section into year, month, and date
				let dividedName = backupName.split('-');
				let year = dividedName[2];
				let month = dividedName[3] - 1; //Months are in 0-11 format
				let day = dividedName[4].split('_')[0];

				// Return the date object
				let date = new Date(year, month, day);

				// If the date is not valid, reject
				if(isNaN(date)) {
					throw new Error('Invalid date');
				}

				// Resolve the date
				resolve(date);	
			} catch (error) {
				reject(error);
			}
		});
	},

	/**
	 * Deletes a version-protected object from GCP
	 * @param {*} bucketName The bucket that contains the object
	 * @param {*} filePath The filepath to the object
	 * @param {*} versionId The version ID of the object to be deleted
	 * @param {*} credentials The GCP credentials
	 * @returns Returns a message when the object is successfully deleted
	 */
	deleteObjectVersion(bucketName, filePath, versionId, credentials) {
		return new Promise((resolve, reject) => { 
			// Create the GCP client and bucket object
			let gcs = new Storage(credentials);
			let bucket = gcs.bucket(bucketName);

			// Check if the bucket exists
			bucket.exists(function (error, exists) {
				if (exists) {
					// Create the file object, and delete it from GCP
					let fileObject = bucket.file(filePath, {generation: versionId});
					fileObject.delete().then(() => {
						resolve('Deleted ' + bucketName + '/' + filePath);
					}).catch(error => {
						reject(error);
					});
				} else {
					if (error) {
						return reject(error);
					} else {
						return reject(new Error('deleteObjectVersion Error: Bucket not found!'));
					}
				}
			});
		});
	},

	/**
	 * Deletes all files in a version-protected bucket or folder. If a bucket is specified, it also deletes the bucket
	 * @param {*} bucketName The bucket name from which to delete files
	 * @param {*} folder Optionally, a folder to delete all files from. If this value is '', the entire bucket is deleted
	 * @param {*} credentials The GCP credentials
	 * @returns Returns a message when all objects are sucessfully deleted
	 */
	deleteAll(bucketName, folder, credentials) {
		return new Promise((resolve, reject) => { 
			try {
				// Create the GCP client and bucket object
				let gcs = new Storage(credentials);
				let bucket = gcs.bucket(bucketName);

				// Find all files in the bucket or folder
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
						// Once all files have been deleted, if the bucket was passed in to be deleted, delete the bucket
						if(folder === '') {
							this.listFileVersions(bucketName, '', credentials).then(remainingFiles => {
								let remainingFileCount = remainingFiles.length;
								if(remainingFileCount === 0) {
									bucket.delete().then(() => {
										resolve('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder + ' and deleted bucket successfully');
									}).catch(error => {
										reject('Deleted ' + itemsDeletedCount + ' objects from ' + bucketName + '/' + folder + ', but experienced error deleting bucket: ' + error);
									});
								}
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

					// Add each file to the queue to be deleted
					for(let curFile of files) {
						let path = curFile.name;
						let versionId = curFile.generation;
						let task = {bucketName, path, versionId, credentials};
						items.push(task);
					}

					// Push all items to be deleted into the queue at once
					itemQueue.push(items);

				}).catch(error => {
					reject(error);
				});
			} catch (error) {
				reject(error);
			}
		});
	},

	/**
	 * Helper function that copies files in GCP between two locations
	 * @param {*} sourceInstallationId The source installation to copy files from
	 * @param {*} sourceCredentials The credentials of the source installation - used for creating the GCP client
	 * @param {*} destinationInstallationId The destination installation to copy files to
	 * @param {*} filesObject An object containing all public and private files that will be copied
	 * @param {*} logger The logger to print output to
	 * @returns If successful, resolves without output
	 */
	transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, filesObject) {
		return new Promise((resolve, reject) => {
			try{
				let publicObjects = filesObject['public'];
				let privateObjects = filesObject['private'];
				let allObjects = publicObjects.concat(privateObjects);

				// Creates the S3 client object
				let client = new Storage(sourceCredentials);
				let sourceBucket = client.bucket(sourceInstallationId);
				let destinationBucket = client.bucket(destinationInstallationId);

				// Check if the source bucket exists
				sourceBucket.exists((error, exists) => {
					if (error) { 
						return reject('Error finding source bucket: ' + error);
					} else if (!exists) {
						return reject('Bucket ' + sourceInstallationId + ' does not exist');
					}

					// Check if the destination bucket exists
					destinationBucket.exists((error, exists) => {
						if (error) { 
							return reject('Error finding destination bucket: ' + error);
						} else if (!exists) {
							return reject('Bucket ' + destinationInstallationId + ' does not exist');
						}

						// Create a queue for processing copy commands, 25 at a time
						let copyCommands = [];
						let itemQueue = async.queue(function(task, done) {
							let fileObject = sourceBucket.file(task.filePath, {generation: task.versionId});
							let fileDestination = destinationBucket.file(task.filePath, {generation: task.versionId});

							fileObject.copy(fileDestination).then(() => {
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

						let compareObject = {};
						// Process each object from the backup
						for(let currentObject of allObjects) {
							try {
								// Create the file's copy source
								let filePath = currentObject['name'];

								// Create the file's destination bucket and key
								let versionId = currentObject['generation'];

								// Find the most recent file version - this will be the one that was active at the time of the backup
								if(filePath in compareObject) {
									if(compareObject[filePath] < versionId){
										compareObject[filePath] = versionId;
									}
								} else {
									compareObject[filePath] = versionId;
								}

							} catch (err) {
								console.log('Error retrieving copy data for ' + currentObject['name'], err);
							}
						}

						// Copy only the most recent files, so that older versions are not made current by copying them
						Object.keys(compareObject).forEach(key => {
							// Create copy parameters and add it to the array that will be put into the processing queue
							let copyParameters = {filePath: key, versionId: compareObject[key]};
							copyCommands.push(copyParameters);
						});

						// Push all copy commands into the queue at once
						itemQueue.push(copyCommands);
					});
				});
			} catch(error) { 
				return reject(error);
			}
		});
	}
};
