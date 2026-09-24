#!/usr/bin/env python3
"""CDK app: one stack, one region. Deploy with the SSO profile:

    cd infra && npx aws-cdk@2 deploy --profile specimux-cloud
"""
import os

import aws_cdk as cdk

from stack import SpecimuxCloudStack

app = cdk.App()
SpecimuxCloudStack(
    app, "specimux-cloud",
    env=cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
                        region=os.environ.get("CDK_DEFAULT_REGION", "us-west-2")),
)
app.synth()
