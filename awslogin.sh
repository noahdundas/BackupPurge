# This is a brief bash script to log a user into a terminal that requires AWS credentials.

# By providing their username and a two-factor authentication code, the script automatically sets up their access key, secret key, and 
# session token to allow the user to use AWS functions in their terminal.

#!/bin/bash
if [ $# == 2 ]; then
	keys=$(aws sts get-session-token --serial-number arn:aws:iam::[code]:mfa/"$1"@company.com --token-code "$2")
	if [[ $? -ne 0 ]] ; then
		echo "Error: Failed to retrieve AWS tokens"
		return 1
	fi
else
	echo "Usage: awslogin [username] [aws mfa]"
        return 1	
fi

creds=$(echo "$keys" | jq -r '.Credentials') 
if [[ $? -ne 0 ]] ; then
	echo "Error: Failed to retrieve Credentials from AWS json"
	echo "Output was: "$keys""
	return 1
fi

accessKey=$(echo "$creds" | jq -r '.AccessKeyId') 
if [[ $? -ne 0 ]] ; then
	echo "Error: Failed to retrieve Access Key Id from AWS json"
	echo "Output was: "$creds""
	return 1
fi

secretKey=$(echo "$creds" | jq -r '.SecretAccessKey') 
if [[ $? -ne 0 ]] ; then
	echo "Error: Failed to retrieve Secret Access Key from AWS json"
	echo "Output was: "$creds""
	return 1
fi

sessionToken=$(echo "$creds" | jq -r '.SessionToken')
if [[ $? -ne 0 ]] ; then
	echo "Error: Failed to retrieve Session Token from AWS json"
	echo "Output was: "$creds""
	return 1
fi

export AWS_ACCESS_KEY_ID="$accessKey"
export AWS_SECRET_ACCESS_KEY="$secretKey"
export AWS_SESSION_TOKEN="$sessionToken"

echo "Login Successful"
