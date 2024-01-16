# This is a small bash scipt designed to allow a user to easily assess the size of all buckets in their AWS account.
# Though used mainly as a quick check for debug and development purposes, it can very easily be modified to serve any number of functions.

#!/usr/bin/bash 
set +m
function calcs3bucketsize() {
    sizeInBytes=`aws s3 ls s3://"${1}" --recursive --human-readable --summarize | awk END'{print}'`
    echo ${1},${sizeInBytes} >> allregions-buckets-s3-sizes.csv
    printf "Calculated size of bucket ${1}: %s\n " "${sizeInBytes}"  
}

[ -f allregions-buckets-s3-sizes.csv ] && rm -fr allregions-buckets-s3-sizes.csv
buckets=`aws s3 ls | awk '{print $3}'`
for j in ${buckets}; do
    # To expedite the calculation, make the cli commands run parallel in the background
    calcs3bucketsize ${j} &
done

wait
set -m
echo "Finished!"
