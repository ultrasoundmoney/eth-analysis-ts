#!/bin/bash

set -e

COMMIT=$(git rev-parse --short HEAD)
SERVICE=$1

if [ -z "$SERVICE" ]; then
  echo "pass service argument, 'serve-fees' or 'serve-fees-ropsten'"
  exit 1;
fi

if [ -n "$(git status --porcelain)" ]; then
  # Uncommitted changes
  read -p "> working directory dirty, continue deploying? " -n 1 -r
  echo    # move to a new line
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    # Early exit, don't deploy dirty directory
    exit 0
  fi
fi

echo -n "> deploying: "
echo $(git show --oneline --no-patch)
echo "> building image"
echo -n "> image hash: "
docker build -q -t gas-analysis .
docker tag gas-analysis 198772674094.dkr.ecr.us-east-2.amazonaws.com/gas-analysis:$COMMIT
echo -n "> pushing image, id.. "
docker push -q 198772674094.dkr.ecr.us-east-2.amazonaws.com/gas-analysis:$COMMIT

update_task() {
  echo -n "> creating a new task revision, rev.. "
  # We need to remove a whole bunch of fields here to make sure our definition
  # gets accepted later when registring a new task definition.
  # See: https://github.com/aws/aws-cli/issues/3064#issuecomment-504681953
  local NEW_TASK_DEFINITION=$(aws ecs describe-task-definition --task-definition $1 | jq ".taskDefinition | .containerDefinitions[0].image = \"198772674094.dkr.ecr.us-east-2.amazonaws.com/gas-analysis:$COMMIT\" | del(.taskDefinitionArn) | del(.revision) | del(.status) | del(.requiresAttributes) | del(.compatibilities) | del(.registeredAt) | del(.registeredBy)")

  # This command immediately returns the newly created task definition.
  local NEW_REVISION=$(aws ecs register-task-definition --family $1 --cli-input-json "$NEW_TASK_DEFINITION" | jq '.taskDefinition.revision')

  echo $NEW_REVISION
}

if [ "$SERVICE" = "serve-fees" ]; then
  NEW_REVISION=$(update_task "$SERVICE")

  echo $NEW_REVISION

  echo "> updating the ecs service"
  aws ecs update-service --cluster ultrasound --service "$SERVICE" --task-definition "$SERVICE" > /dev/null

  echo "> waiting for $SERVICE to stabilize"
  aws ecs wait services-stable --cluster ultrasound --services "$SERVICE"

  echo "> done"
elif [ "$SERVICE" = "serve-fees-ropsten" ]; then
  NEW_REVISION=$(update_task "$SERVICE")

  echo $NEW_REVISION

  echo "> updating the ecs service"
  aws ecs update-service --cluster ultrasound --service "$SERVICE" --task-definition "$SERVICE" > /dev/null

  echo "> waiting for $SERVICE to stabilize"
  aws ecs wait services-stable --cluster ultrasound --services "$SERVICE"

  echo "> done"
fi
fi
