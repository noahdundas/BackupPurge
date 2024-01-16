const { installation } = require('@user-company/company-node-utils');
const FileUtils = require('@user-company/company-node-utils').files;
const clusterUtils = require('@user-company/company-node-utils').cluster;
const async = require('async');

// The script will delete all but the last quantity of backups from across all filesources
const quantity = 30;

// Set the dryrun variable to determine if we should truly delete bucket contents
let dryrun = process.env.DRYRUN || true;

if(typeof dryrun === 'string'){
	dryrun = dryrun.toLowerCase();
	if(dryrun === 'false'){
		dryrun = false;
	} else {
		dryrun = true;
	}
}

// Retrieve all installations from Consul
installation.getInstallationIdList().then(async function(installations) {
	// Retrieve all filesources
	clusterUtils.getClusterConfig().then(async clusterConfig => {
		// Get filesources from clusterData
		let filesources = Object.keys(clusterConfig['filesource']);

		for(let currentInstallation of installations){

			// Initialize an array that will hold promises
			let promises = [];

			// For each filesource...
			for(let filesource of filesources){		
				// Retrieve the backups held on the filesource
				await FileUtils.listBackups(currentInstallation, filesource).then(backups => {
					// Print the number of backups found
					console.log(currentInstallation + ': Found ' + backups.length + ' backups from ' + filesource);
				
					// Calculate the date for each backup and add that promise to the promise array
					for(let currentBackup of backups) {
						promises.push(new Promise((resolve) => {
							FileUtils.calculateDate(currentInstallation, currentBackup, filesource).then(curDate => {
								resolve({name: currentBackup, date: curDate, filesource: filesource});
							}).catch(error => {
								console.warn('Omitting ' + currentBackup + ' from purge, date could not be sorted. Error: ' + error);
								resolve({name: currentBackup, date: undefined, filesource: filesource});
							});
						}));
					}
				}).catch(error => {
					console.log(currentInstallation + ': Errored while finding backups for ' + filesource + ': ' + error);
				});
			}	

			// Once all date calculations resolve, we have an array of dated backup objects
			Promise.all(promises).then(async (backupObjects) => {
				let backupsToKeep = [];

				// Exclude any unsortable backups from the valid backups array
				backupObjects.forEach(function (currentBackupObject, i) {
					if(currentBackupObject.date === undefined){
						if(dryrun !== false){
							backupsToKeep.push(backupObjects[i]);
						}
						backupObjects.splice(i, 1);
					}
				});

				// If there are not enough backups to purge, skip to the next installation
				if((backupObjects.length) <= quantity){
					console.log(currentInstallation + ': Not enough backups to purge');
					return;
				}

				// Sort the valid backups by date
				backupObjects = await backupObjects.sort(function(a,b){
					let dateA = 0;
					let dateB = 0;
					try{
						dateA = a.date.getTime();
						if(isNaN(dateA)) {
							throw new Error('Invalid date');
						}
					} catch (err) {
						dateA = Infinity;
					}
					try{
						dateB = b.date.getTime();
						if(isNaN(dateB)) {
							throw new Error('Invalid date');
						}
					} catch (err) {
						dateB = Infinity;
					}
					return dateA - dateB;
				});

				// Separate the most recent quantity of backups from the backups to delete
				let backupsToDelete = backupObjects.slice(0, -quantity);
				if(dryrun !== false) {
					backupsToKeep = backupsToKeep.concat(backupObjects.slice(-quantity));
				}

				// Initialize an array to store all deletion promises
				let deleteBackups = [];

				// Create an async await queue that creates creates a delete object promise for each item it processes
				let itemQueue = async.queue(function(task, done) {
					FileUtils.deleteAll(task.currentInstallation, task.path, task.filesource).then(result => {
						console.log(result);
						done();
					}).catch(error => {
						console.log(task.currentInstallation + ": DeleteAll error: " + error);
						done();
					});
				}, 10);

				// Set the output message for when the queue empties
				itemQueue.drain(() => {
					console.log('Purge complete');
				});

				// If we are deleting backups for real, create the delete command for each backup to delete and add it to the array
				if(dryrun === false){
					for(let currentBackup of backupsToDelete) {
						let path = 'backups/'+currentBackup.name;
						let filesource = currentBackup.filesource;
						let task = {currentInstallation, path, filesource};
						deleteBackups.push(task);
					}
				} else {
					// Otherwise, just print the targets for deletion
					console.log(currentInstallation + ': Dryrun wants to delete ' + backupsToDelete.length + ' backups:');
					for(let backup of backupsToDelete){
						console.log('Delete: ' + backup.name + ', filesource: ' + backup.filesource);
					}
					console.log(currentInstallation + ': Dryrun wants to keep ' + backupsToKeep.length + ' backups');
					for(let backup of backupsToKeep){
						console.log('Keep: ' + backup.name + ', filesource: ' + backup.filesource);
					}
				}

				// Push all items to be deleted into the queue at once
				itemQueue.push(deleteBackups);

			}).catch(error => {
				console.log(currentInstallation + ': Errored while calculating dates: ' + error);
			});
		}
	}).catch(error => {
		console.log('Error retrieving filesource list: ' + error);
	});
}).catch((error) => {
	console.log('Error retrieving installation ID list: ' + error);
});
