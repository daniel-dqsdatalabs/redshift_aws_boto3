import boto3
import json
import pandas as pd
import configparser

'''
    create the aws resources needed to run project
'''
   

########################
#       FIREWALL
########################

def open_firewall(ec2, vpc_id, config):
    vpc = ec2.Vpc(id = vpc_id)
    defaultSg = list(vpc.security_groups.all())[0]
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name, 
        CidrIp='0.0.0.0/0',  
        IpProtocol='TCP',  
        FromPort=int(config.get("DWH","DB_PORT")),
        ToPort=int(config.get("DWH","DB_PORT"))
    )

########################
#       SVC/RSC
########################

def get_instance(service, config):
    return boto3.client(
        service, 
        region_name = 'sa-east-1', 
        aws_access_key_id = config.get("AWS","KEY"), 
        aws_secret_access_key = config.get("AWS","SECRET")
    )   

def get_rsrc_instance(resource, config):
    return boto3.resource(
        resource, 
        region_name = 'sa-east-1', 
        aws_access_key_id = config.get("AWS","KEY"), 
        aws_secret_access_key = config.get("AWS","SECRET")
    )   

def delete_enviroment(redshift, iam, config):
    redshift.delete_cluster(
        ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),  
        SkipFinalClusterSnapshot=True
    )
    
    iam.detach_role_policy(
        RoleName = config.get("DWH","DWH_IAM_ROLE_NAME"),
        PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    
    iam.delete_role(RoleName = config.get("DWH","DWH_IAM_ROLE_NAME"))
    

########################
#       IAM
########################

def create_aim_role(iam, config):
    return iam.create_role(
        Path = '/',
        RoleName = config.get("DWH","DWH_IAM_ROLE_NAME"), 
        AssumeRolePolicyDocument = json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow', 
                'Principal': {
                    'Service': 'redshift.amazonaws.com'
                }
            }
            ],
            'Version': '2012-10-17'
        })
    )

 
def get_role_arn(iam, config):
    return iam.get_role(
        RoleName = config.get("DWH","DWH_IAM_ROLE_NAME"))['Role']['Arn']
   

def attach_policy(iam, config):
    iam.attach_role_policy(
        RoleName = config.get("DWH","DWH_IAM_ROLE_NAME"), 
        PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']


########################
#       REDSHIFT
########################

def create_redshift(redshift, role, config):
    return redshift.create_cluster(        
        ClusterType = config.get("DWH","DWH_CLUSTER_TYPE"), 
        NodeType = config.get("DWH","DWH_NODE_TYPE"), 
        NumberOfNodes = int(config.get("DWH","DWH_NUM_NODES")),
        DBName = config.get("DWH","DWH_DB"),
        ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
        MasterUsername = config.get("DWH","DWH_DB_USER"),
        MasterUserPassword = config.get("DWH","DWH_DB_PASSWORD"),
        IamRoles = role
    )


########################
#       START
########################

def main():
    '''
        reads the config file and creates AWS services/resources
    '''
    try:
        # config file
        config = configparser.ConfigParser()
        config.read_file(open('dwh.cfg'))
    
        # get services/resource clients
        s3_cli = get_rsrc_client('s3', config)
        ec2_cli = get_rsrc_client('ec2', config)
        iam_cli = get_serv_client('iam', config)
        redshift_cli = get_serv_client('redshift', config)
        
        #create the services
        
        iam_obj = create_aim_role(config)
        attach_policy(iam_cli, config)
        
        role_obj = get_role_arn(iam_cli, config)
        redshift_obj = create_redshift(redshift_cli, role_obj, config)
        
        vpc_id = redshift.get_prop() ##falta adicionar  #myClusterProps['VpcId'])
        open_firewall(ec2_cli, vpc_id, config)
        
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
