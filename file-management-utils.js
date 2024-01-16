const gcsUtils = require('../gcs/google-cloud-storage-v2');
const s3Utils = require('../amazon/amazon-s3-utils');
const getFileConnectorInfo = require('../cluster/cluster-utils').getFileConnectorInfo;
const installationUtils = require('../installation/installation-utils');
const fs = require('fs');

/**
 * Util Class to manage File Systems tasks 
 */
const files = {	
	/**
	 * Lists all files in a specified bucket, with their versioning information
	 * @param {*} installationId The installation ID to find the bucket for
	 * @param {*} folder A folder to limit results by - set to '' for all bucket contents
	 * @param {*} filesource The filesource to draw data from
	 * @returns Returns a collection of file objects in the format provided by the datasource
	 */
	listFileVersions(installationId, folder, filesource) {
		return new Promise((resolve, reject) => {
			getFileConnectorInfo(filesource, installationId + '.company.com')
				.then(({providerInfo: {provider, product, credentials}}) => {
					switch(provider) {
						case 'google': {
							switch(product) {
								case 'storage': {
									return gcsUtils.listFileVersions(installationId, folder, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('listFiles error: method does not currently support ' + provider + '\'s ' + product));
								}
							}
						}
						case 'amazon': {
							switch(product) {
								case 's3': {
									return s3Utils.listFileVersions(installationId, folder, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('listFileVersions error: method does not currently support ' + provider + '\'s ' + product));
								}

							}
						}
						default: {
							return reject(new Error('listFileVersions error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				})
				.catch(reject);
		});
	},

	/**
	 * Lists all folder names found in the backups/ folder of the installation
	 * @param {*} installationId The installation ID to find the bucket for
	 * @param {*} filesource The filesource to draw data from
	 * @returns Returns an array of backup name strings
	 */
	listBackups(installationId, filesource) {
		return new Promise((resolve, reject) => {
			getFileConnectorInfo(filesource, installationId + '.company.com')
				.then(({providerInfo: {provider, product, credentials}}) => {
					switch(provider) {
						case 'google': {
							switch(product) {
								case 'storage': {
									return gcsUtils.listBackups(installationId, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('listFiles error: method does not currently support ' + provider + '\'s ' + product));
								}
							}
						}
						case 'amazon': {
							switch(product) {
								case 's3': {
									return s3Utils.listBackups(installationId, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('listBackups error: method does not currently support ' + provider + '\'s ' + product));
								}

							}
						}
						default: {
							return reject(new Error('listBackups error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				})
				.catch(reject);
		});
	},

	/**
	 * Calculates the date of a backup from its generated name
	 * @param {*} installationId The installation ID for getting connector info
	 * @param {*} backupName The backup name to extract the date from
	 * @param {*} filesource The filesource to draw provider and product data from 
	 * @returns Returns a Date object
	 */
	calculateDate(installationId, backupName, filesource) {
		return new Promise((resolve, reject) => {
			getFileConnectorInfo(filesource, installationId + '.company.com')
				.then(({providerInfo: {provider, product}}) => {
					switch(provider) {
						case 'google': {
							switch(product) {
								case 'storage': {
									return gcsUtils.calculateDate(backupName)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('calculateDate error: method does not currently support ' + provider + '\'s ' + product));
								}
							}
						}
						case 'amazon': {
							switch(product) {
								case 's3': {
									return s3Utils.calculateDate(backupName)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('calculateDate error: method does not currently support ' + provider + '\'s ' + product));
								}

							}
						}
						default: {
							return reject(new Error('calculateDate error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				})
				.catch(reject);
		});
	},

	/**
	 * Deletes a single version-protected object from the filesource
	 * @param {*} installationId The installation ID to find the bucket for
	 * @param {*} objectPath The path to the object within the bucket
	 * @param {*} versionId The version of the object to delete
	 * @param {*} filesource The filesource to remove data from
	 * @returns Returns a success output message when the process finishes
	 */
	deleteObjectVersion(installationId, objectPath, versionId, filesource) {
		return new Promise((resolve, reject) => {
			getFileConnectorInfo(filesource, installationId + '.company.com')
				.then(({providerInfo: {provider, product, credentials}}) => {
					switch(provider) {
						case 'google': {
							switch(product) {
								case 'storage': {
									return gcsUtils.deleteObjectVersion(installationId, objectPath, versionId, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('deleteObjectVersion error: method does not currently support ' + provider + '\'s ' + product));
								}
							}
						}
						case 'amazon': {
							switch(product) {
								case 's3': {
									return s3Utils.deleteObjectVersion(installationId, objectPath, versionId, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('deleteObjectVersion error: method does not currently support ' + provider + '\'s ' + product));
								}

							}
						}
						default: {
							return reject(new Error('deleteObjectVersion error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				})
				.catch(reject);
		});
	},

	/**
	 * Deletes all files in a version-protected folder or bucket
	 * @param {*} installationId The installation ID to find the bucket for
	 * @param {*} folder The folder to delete - if this is '', the entire bucket will be deleted instead
	 * @param {*} filesource The filesource to remove data from
	 * @returns Returns a success output message when the process finishes
	 */
	deleteAll(installationId, folder, filesource) {
		return new Promise((resolve, reject) => {
			getFileConnectorInfo(filesource, installationId + '.company.com')
				.then(({providerInfo: {provider, product, credentials}}) => {
					switch(provider) {
						case 'google': {
							switch(product) {
								case 'storage': {
									return gcsUtils.deleteAll(installationId, folder, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('deleteAll error: method does not currently support ' + provider + '\'s ' + product));
								}
							}
						}
						case 'amazon': {
							switch(product) {
								case 's3': {
									return s3Utils.deleteAll(installationId, folder, credentials)
										.then(resolve)
										.catch(reject);
								}
								default: {
									return reject(new Error('deleteAll error: method does not currently support ' + provider + '\'s ' + product));
								}

							}
						}
						default: {
							return reject(new Error('deleteAll error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				})
				.catch(reject);
		});
	},

	/**
	 * Transfers files from one installation to another - if the source and destination installation are the same, 
	 * it will restore the version of each file to what was recorded in the backup
	 * @param {*} sourceInstallationId The source installation that is being restored from
	 * @param {*} sourceFilesource The filesource of the source installation
	 * @param {*} destinationInstallationId The destination installation that is recieving the restore
	 * @param {*} backupFolder The name of the backup that is being restored
	 * @returns If successful, resolves without output
	 */
	transferFiles(sourceInstallationId, sourceFilesource, destinationInstallationId, backupFolder) {
		return new Promise((resolve, reject) => {
  
			// Retrieve and download the files.json file from the backup
			let cwd = process.cwd();
			this.backupDownloadFile(sourceInstallationId, backupFolder, 'files.json', cwd + '/' + backupFolder + '/files.json', sourceFilesource).then(() => {
				// Read the files.json file
				fs.readFile(cwd + '/' + backupFolder + '/files.json', (err, data) => {
					if (err){
						return reject(err);
					}
  
					// Record the data from the files.json file
					let fileData = JSON.parse(data);
  
					// Clean up the files.json file
					fs.unlink(cwd+'/'+backupFolder+'/files.json', () => {
						// Retrieve the destination filesource
						let fqdn = destinationInstallationId + '.company.com'; 
						installationUtils.getInstallationConfig(fqdn)
							.then(destinationConfig => {
								let destinationFilesource = destinationConfig.filesource;
								// Retrieve the destination filesource's credentials
								return getFileConnectorInfo(destinationFilesource, destinationInstallationId + '.company.com');
							})
							.then(({providerInfo: {provider, product, credentials}}) => {
								// Parse the destination's credentials
								let destinationProvider = provider;
								let destinationProduct = product;
								let destinationCredentials = credentials;
								// Retrieve the source filesource's credentials
								getFileConnectorInfo(sourceFilesource, sourceInstallationId + '.company.com')
									.then(({providerInfo: {provider, product, credentials}}) => {
										// Parse the source's credentials
										let sourceProvider = provider;
										let sourceProduct = product;
										let sourceCredentials = credentials;
  
										// Compare the source and destination filesource - if they match, we can transfer using the filesource's utils
										// Otherwise, we need to download and re-upload all files, which may be more expensive
										if(destinationProvider === sourceProvider && destinationProduct === sourceProduct) {
											this.transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, destinationCredentials, fileData, sourceProvider, sourceProduct).then(() => {
												return resolve();
											}).catch(error => {
												return reject(error);
											});
										} else {
											return reject('File transfer between different filesource providers / products is not currenly supported');
										}
									}).catch(error => {
										return reject(error);
									});
							}).catch(error => {
								return reject(error);
							});
					});
				});
			}).catch(error => {
				return reject(error);
			});
		});
	},

	/**
	 * Copies files from one folder or bucket to another
	 * @param {*} sourceInstallationId The source installation ID
	 * @param {*} filesource The source's filesource
	 * @param {*} destinationInstallationId The destination installation ID
	 * @param {*} destinationCredentials The destination's filesource
	 * @param {*} filesObject An object containing all public and private files - only the current versions of which will be copied to avoid versioning collisions
	 * @returns If successful, resolves without output
	 */
	transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, destinationCredentials, filesObject, provider, product) {
		return new Promise((resolve, reject) => {
			// Credentials are provided by calling function
			switch(provider) {
				case 'google': {
					switch(product) {
						case 'storage': {
							return gcsUtils.transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, filesObject)
								.then(resolve)
								.catch(reject);
						}
						default: {
							return reject(new Error('transferBackupFiles error: method does not currently support ' + provider + '\'s ' + product));
						}
					}
				}
				case 'amazon': {
					switch(product) {
						case 's3': {
							return s3Utils.transferBackupFiles(sourceInstallationId, sourceCredentials, destinationInstallationId, destinationCredentials, filesObject)
								.then(resolve)
								.catch(reject);
						}
						default: {
							return reject(new Error('transferBackupFiles error: method does not currently support ' + provider + '\'s ' + product));
						}
	
					}
				}
				default: {
					return reject(new Error('transferBackupFiles error: method does not currently support ' + provider + '\'s ' + product));
				}
			}
		});
	}
};

module.exports = files;
