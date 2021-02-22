import boto3
import csv
from collections import defaultdict
from inventools import  tools
from  concurrent.futures import  ThreadPoolExecutor,ProcessPoolExecutor
import os
import pandas as pd
import glob
import json
from functools import wraps,reduce



## Need merged the csv , elastic cache , snasphots , rds, workspace,vpn tunnel, vpc peering




class inventory:
    instance_header=['s_no','regions','instance_name','instance_imageid','instance_id','instance_type','eip_attached','instance_volume_id','Snapshot_count','instance_vpc_id','instance_security_grpname','instance_current_state']
    elb_header=['s_no','regions','lb_name','instance','type']
    elb2_header=['s_no','region','lb_name','lb_arn','lb_type','ListenerArn','TargetGroupArn','tg_instance']
    network_header=["s_no","region","vpcid","vpc_cidr","subnet_id","subnet_cidr","public","routetable_id"]    
    gateway_header=["s_no","region","igwId","NatGatewayId"]
    eip_header=["s_no","region","eip","eip_association"]
    instance_audit_header=["s_no","region","instance_id","Event_name","User_name","Access_key"]
    snapshot_header=["s_no","region","SnapshotId","Description","OwnerId","VolumeId","StartTime"]
    elasticache_header=["s_no","_list_region","CacheClusterId","CacheNodeType","Engine","EngineVersion","CacheClusterStatus","NumCacheNodes","CacheSubnetGroupName","ReplicationGroupId","SnapshotName","SnapshotStatus","SnapshotSource","CacheNodeType","VpcId"]
    rds_DB_header=["s_no","_list_region","DBInstanceIdentifier","DBInstanceClass","Engine","MasterUsername","Address","Port","AllocatedStorage","InstanceCreateTime","InstanceCreateTime","StorageType","AutoMinorVersionUpgrade","DBParameterGroupName","VpcSecurityGroupId","VpcId","SubnetIdentifier","MultiAZ","LicenseModel","CACertificateIdentifier","snapshot"]
    rds_snapshot_header=["s_no","_list_region",'DBInstanceIdentifier','DBSnapshotIdentifier','SnapshotCreateTime','Encrypted','SnapshotType']
    s3_bucket_header=["s_no","bucket_name","bucket_size"]

    def __init__(self):
        ec2=boto3.client('ec2')
        self.list_region = [x['RegionName'] for x in ec2.describe_regions()['Regions']] 


    @tools.write_csv('inventory',instance_header)
    #@tools.write_json('inventory',instance_header)
    def instance_inv(self):
        s_no=1
        for _list_region in self.list_region:
            ec2=boto3.client('ec2', region_name=_list_region)        
            for x in ec2.describe_instances()['Reservations']:
                try:
                    for inst in x['Instances']:
                        ins_imgid=inst['ImageId']
                        ins_id=inst['InstanceId']
                        filters = [{'Name': 'instance-id', 'Values': [ins_id]}]
                        eip_res = ec2.describe_addresses(Filters=filters)
                        eip_list=[]
                        eip_len=len(eip_res['Addresses'])
                        for r in range(0,eip_len):
                            eip_list.append(eip_res['Addresses'][r]['PublicIp'])
                        ins_type=inst['InstanceType']
                        ins_state=inst['State']['Name']
                        '''  filtering  in Tags [{'Key': 'Name', 'Value': 'linuxprod'}, {'Key': 'project', 'Value': 'test'}]  with help of len function checking total length
                        then using range funtion passing as array index, checking in  dictionary key should be 'Name, if tag key as Name then  storing value in variable'
                        '''
                        try:
                            tag_len=len(inst['Tags'])
                            for r in range(0,tag_len):
                                if inst['Tags'][r]['Key'] == 'Name':
                                    ins_name=inst['Tags'][r]['Value']
                        except:
                            ins_name = "Not_defined"
                        ins_vpcid=inst['VpcId']
                        for ivol in inst['BlockDeviceMappings']:
                            ins_volid=ivol['Ebs']['VolumeId']
                        filters = [{'Name': 'volume-id', 'Values': [ins_volid]}]
                        snap_no = ec2.describe_snapshots(Filters=filters)
                        no_snapshot=len(snap_no['Snapshots'])
                        for isec in inst['SecurityGroups']:
                            ins_secgrp=isec['GroupName']
                    yield([s_no,_list_region,ins_name,ins_imgid,ins_id,ins_type,eip_list,ins_volid,no_snapshot,ins_vpcid,ins_secgrp,ins_state])
                    s_no=s_no+1
                except:
                    yield([s_no,_list_region,"null",ins_imgid,ins_id,ins_type,eip_list,'null','null','null','null',ins_state])
                    s_no=s_no+1
                                    
                                    
    @tools.write_csv('elb_classic',elb_header)
    #@tools.write_json('elb_classic',elb_header)
    def des_elb(self):
        s_no=1
        for _list_region in self.list_region:
            elb = boto3.client('elb', region_name=_list_region) 
            response= elb.describe_load_balancers()['LoadBalancerDescriptions']   
            elb={}     
            for x in response:            
                temp=[]
                for y in x['Instances']:
                    temp.append(y["InstanceId"])
                elb[x['LoadBalancerName']]=temp            
            for x,y in elb.items():
                yield([s_no,_list_region,x,y,'classic'])
                s_no=s_no+1
                 
    @tools.write_csv('elbv2',elb2_header)
    #@tools.write_json('elbv2',elb2_header)
    def des_elb_v2(self):    
        s_no=1
        for _list_region in self.list_region:    
            elb2 = boto3.client('elbv2', region_name=_list_region)
            try: 
                response= elb2.describe_load_balancers()['LoadBalancers']            
                for x in response:
                    tg=defaultdict(list)                  
                    tg['region']=_list_region
                    tg['lb_name']=x['LoadBalancerName']
                    tg['lb_arn']=x['LoadBalancerArn']
                    tg['lb_type']=x['Type']
                    response_ = elb2.describe_listeners(LoadBalancerArn=x['LoadBalancerArn'])['Listeners']
                    try:        
                        for y in response_:        
                            tg['ListenerArn'].append(y['ListenerArn'])
                            tg['TargetGroupArn'].append(y['DefaultActions'][0]['TargetGroupArn'])
                            _response = elb2.describe_target_health(TargetGroupArn=y['DefaultActions'][0]['TargetGroupArn'])
                            try:
                                tag_len=len(_response['TargetHealthDescriptions'])
                                for r in range(0,tag_len):
                                    tg['tg_instance'].append(_response['TargetHealthDescriptions'][r]['Target']['Id'])
                            except:
                                tg['ListenerArn']="Null"
                                tg['TargetGroupArn']="Null"
                                tg['tg_instance']='Null'                   
                    except :
                            tg['ListenerArn']="Null"
                            tg['TargetGroupArn']="Null"
                            tg['tg_instance']='Null'
                    yield([s_no,tg['region'],tg['lb_name'],tg['lb_arn'],tg['lb_type'],tg['ListenerArn'],tg['TargetGroupArn'],tg['tg_instance']])
                    s_no=s_no+1
            except Exception as e:
                print("Error:",e)
        


    @tools.write_csv('network',network_header)
    #@tools.write_json('network',network_header)
    def network(self):
        s_no=1
        for _list_region in self.list_region:    
            ec2 = boto3.client('ec2',region_name=_list_region)
            vpc_res = ec2.describe_vpcs()
            for x in vpc_res['Vpcs']:
                vpc_cdr=x['CidrBlock']
                vpcid=x['VpcId']    
                filters_vpc=[{'Name': 'vpc-id','Values': [vpcid]},]
                sub_res = ec2.describe_subnets(Filters=filters_vpc)
                for y in sub_res['Subnets']:
                    sub_cdr=y['CidrBlock']
                    public=y['MapPublicIpOnLaunch']
                    subnet_id=y['SubnetId']
                    filters_route=[{'Name': 'association.subnet-id','Values': [subnet_id]},]
                    route_res = ec2.describe_route_tables(Filters=filters_vpc)
                    routetable_id = [z['RouteTableId'] for z in route_res['RouteTables']]
                    yield([s_no,_list_region,vpcid,vpc_cdr,subnet_id,sub_cdr,public,routetable_id])
                    s_no=s_no+1

        
    @tools.write_csv('eip',eip_header)
    #@tools.write_json('eip',eip_header)
    def eip(self):
        s_no=1
        for _list_region in self.list_region:    
            ec2 = boto3.client('ec2', region_name=_list_region)
            eip_res = ec2.describe_addresses()
            eip_len=len(eip_res['Addresses'])
            for r in range(0,eip_len):
                try:
                    eip=eip_res['Addresses'][r]['PublicIp']
                    try:
                        eip_association=eip_res['Addresses'][r]['AssociationId']
                    except:
                        eip_association='Null'
                except Exception as e:
                    print("Error",e)                     
                yield ([s_no,_list_region,eip,eip_association])
                s_no=s_no+1

    @tools.write_csv('gateway',gateway_header)
    #@tools.write_json('gateway',gateway_header)
    def gateway(self):
        s_no=1
        for _list_region in self.list_region:    
            ec2 = boto3.client('ec2', region_name=_list_region)        
            igw_res=ec2.describe_internet_gateways()       
            igwId =[i['InternetGatewayId'] for i in igw_res['InternetGateways']  ]
            nat_res=ec2.describe_nat_gateways()
            NatGatewayId =[z['NatGatewayId'] for z in nat_res['NatGateways']]
            yield ([s_no,_list_region,igwId,NatGatewayId])
            s_no=s_no+1

    @tools.write_csv('instance_audit',instance_audit_header)
    #@tools.write_json('instance_audit',instance_audit_header)
    def instance_audit(self):
        s_no=1
        for _list_region in self.list_region:  
            cloudtrail = boto3.client('cloudtrail', region_name=_list_region)
            ec2 = boto3.resource('ec2', region_name=_list_region)  
            ins_sid = [] 
            ins_tid = [] 
            for inst in ec2.instances.all():  
                try:
                    if inst.state['Name'] == 'stopped' or inst.state['Name'] == 'terminated': #filtering  the instance if stopped and terminated
                        try: 
                            if inst.state['Name'] == 'stopped':
                                ins_sid.append(inst.id) # appending and storing the instance id in list ins_sid  for stopped instance
                                len_tags=len(inst.tags) # checking len of tag
                                for x in range(0,len_tags):
                                    if inst.tags[x]['Key'].lower() == 'maintenance' and inst.tags[x]['Value'].lower() == 'true': #filtering the tag if key as maintenance and value as true
                                        ins_sid.remove(inst.id) # deleting the instance id in list if condition is true
                                for i in ins_sid: # now we  filtered case stoped , terminated 
                                    i_id='please check the instance id {}'.format(i)
                                    cloud_trail_events = cloudtrail.lookup_events(LookupAttributes=[{'AttributeKey': 'ResourceName','AttributeValue': i},],MaxResults=1,)
                                    for x in cloud_trail_events['Events']:
                                        Access_key="AccessKeyId = {}".format(x['AccessKeyId'])
                                        Event_name="EventName = {}".format(x['EventName'])
                                        User_name="Username = {}".format(x['Username'])  
                                        yield ([s_no,_list_region,i_id,Event_name,User_name,Access_key])
                                        s_no=s_no+1                                
                                        
                        except Exception as e:
                            print("Error",e)
                        try:
                            if inst.state['Name'] == 'terminated':
                                ins_tid.append(inst.id) # appending and storing the instance id in list ins_tid for terminated
                                len_tags=len(inst.tags) # checking len of tag
                                for x in range(0,len_tags):
                                    if inst.tags[x]['Key'].lower() == 'maintenance' and inst.tags[x]['Value'].lower() == 'true': #filtering the tag if key as maintenance and value as true
                                        ins_tid.remove(inst.id) # deleting the instance id in list if condition is true
                                for i in ins_tid: # now we  filtered case stoped , terminated 
                                    i_id='please check the instance id {}'.format(i)
                                    cloud_trail_events = cloudtrail.lookup_events(LookupAttributes=[{'AttributeKey': 'ResourceName','AttributeValue': i},],MaxResults=1,)
                                    for x in cloud_trail_events['Events']:
                                        Access_key="AccessKeyId = {}".format(x['AccessKeyId'])
                                        Event_name="EventName = {}".format(x['EventName'])
                                        User_name="Username = {}".format(x['Username'])
                                        yield ([s_no,_list_region,i_id,Event_name,User_name,Access_key])
                                        s_no=s_no+1     
   
                        except Exception as e:
                            print("Error",e)
                except:
                    print('no instance in stopped and terminated state')

    @tools.write_csv('i_snapshot',snapshot_header)
    #@tools.write_json('i_snapshot',snapshot_header)               
    def i_snapshot(self):
        s_no=1
        for _list_region in self.list_region:  
            ec2 = boto3.client('ec2', region_name=_list_region)             
            snap = ec2.describe_snapshots()
            for x in snap['Snapshots']:
                SnapshotId,Description,OwnerId,VolumeId,StartTime= x['SnapshotId'],x['Description'],x['OwnerId'],x['VolumeId'],str(x['StartTime'])
                yield ([s_no,_list_region,SnapshotId,Description,OwnerId,VolumeId,StartTime])
                s_no=s_no+1

    @tools.write_csv('rds_DB',rds_DB_header)
    #@tools.write_json('rds_DB',rds_DB_header)               
    def rds_DB(self):
        s_no=1        
        for _list_region in self.list_region:    
               rds = boto3.client('rds', region_name=_list_region)               
               response_db = rds.describe_db_instances()['DBInstances']
               for x in response_db:                       
                        DBInstanceIdentifier=x['DBInstanceIdentifier'] if x['DBInstanceIdentifier'] else 'null'
                        DBInstanceClass=x['DBInstanceClass'] if x['DBInstanceClass'] else 'null'
                        Engine= x['Engine'] if x['Engine'] else 'null'
                        MasterUsername=x['MasterUsername'] if x['MasterUsername'] else 'null'
                        Address=x['Endpoint']['Address'] if x['Endpoint']['Address'] else 'null'
                        Port= x['Endpoint']['Port'] if x['Endpoint']['Port'] else 'null'
                        AllocatedStorage = x['AllocatedStorage'] if x['AllocatedStorage'] else 'null'
                        InstanceCreateTime = str(x['InstanceCreateTime']) if x['InstanceCreateTime'] else 'null'
                        StorageType = x['StorageType'] if x['StorageType'] else 'null'
                        try:
                                MaxAllocatedStorage = x['MaxAllocatedStorage'] if x['MaxAllocatedStorage']  else 'null'
                        except :
                                 MaxAllocatedStorage = 'null'
                        AutoMinorVersionUpgrade = x['AutoMinorVersionUpgrade'] if x['AutoMinorVersionUpgrade'] else 'null'                        
                        PubliclyAccessible = x['PubliclyAccessible'] if x['PubliclyAccessible'] else 'null'                      
                        DBParameterGroupName = [z['DBParameterGroupName'] for z in x['DBParameterGroups']]  
                        VpcSecurityGroupId   = [y['VpcSecurityGroupId'] for y in x['VpcSecurityGroups']]
                        VpcId= x['DBSubnetGroup']['VpcId']  if x['DBSubnetGroup']['VpcId']  else 'null'                     
                        SubnetIdentifier=[a['SubnetIdentifier'] for a in x['DBSubnetGroup']['Subnets']]                        
                        MultiAZ=x['MultiAZ'] if x['MultiAZ']  else 'null'
                        LicenseModel=x['LicenseModel'] if x['LicenseModel']  else 'null'          
                        CACertificateIdentifier= x['CACertificateIdentifier']  if x['CACertificateIdentifier']  else 'null'
                        response_snap = rds.describe_db_snapshots( DBInstanceIdentifier=DBInstanceIdentifier)['DBSnapshots']      
                        snapshot=[s['DBSnapshotIdentifier'] for s in response_snap]
                        yield([s_no,_list_region,DBInstanceIdentifier,DBInstanceClass,Engine,MasterUsername,Address,Port,AllocatedStorage,InstanceCreateTime,InstanceCreateTime,StorageType,AutoMinorVersionUpgrade,DBParameterGroupName,VpcSecurityGroupId,VpcId,SubnetIdentifier,MultiAZ,LicenseModel,CACertificateIdentifier,snapshot])
                        s_no=s_no+1

    @tools.write_csv('rds_snapshot',rds_snapshot_header)
    #@tools.write_json('rds_snapshot',rds_snapshot_header)  
    def rds_snapshot(self):
        s_no=1        
        for _list_region in self.list_region:    
                rds = boto3.client('rds', region_name=_list_region)                              
                response_snap = rds.describe_db_snapshots()['DBSnapshots']
                for s in response_snap:                        
                        yield([s_no,_list_region,s['DBInstanceIdentifier'],s['DBSnapshotIdentifier'],str(['SnapshotCreateTime']),s['Encrypted'],s['SnapshotType']])
                        s_no=s_no+1
                        
    @tools.write_csv("elasticache",elasticache_header)
    #@tools.write_json("elasticache",elasticache_header)
    def elastic_cache(self):
        s_no=1
        for _list_region in self.list_region:    
                client = boto3.client('elasticache', region_name=_list_region)    
                cache = client.describe_cache_clusters()['CacheClusters']
                CacheClusterId,CacheNodeType,Engine,EngineVersion,CacheClusterStatus,NumCacheNodes,CacheSubnetGroupName,ReplicationGroupId,SnapshotName,SnapshotStatus,SnapshotSource,CacheNodeType,VpcId= ["None" for x in range(0,13)]
                for x in cache:
                        CacheClusterId,CacheNodeType,Engine,EngineVersion,CacheClusterStatus,NumCacheNodes,CacheSubnetGroupName,ReplicationGroupId=  x['CacheClusterId'],x['CacheNodeType'],x['Engine'],x['EngineVersion'],x['CacheClusterStatus'],x['NumCacheNodes'],x['CacheSubnetGroupName'],x['ReplicationGroupId']
                        cache_snap = client.describe_snapshots(CacheClusterId=CacheClusterId)['Snapshots']              
                        for y in cache_snap:
                                SnapshotName,SnapshotStatus,SnapshotSource,CacheNodeType,VpcId=y['SnapshotName'],y['SnapshotStatus'],y['SnapshotSource'],y['CacheNodeType'],y['VpcId']
                        yield([s_no,_list_region,CacheClusterId,CacheNodeType,Engine,EngineVersion,CacheClusterStatus,NumCacheNodes,CacheSubnetGroupName,ReplicationGroupId,SnapshotName,SnapshotStatus,SnapshotSource,CacheNodeType,VpcId])
                        s_no=s_no+1
    @tools.write_csv("s3_bucket",s3_bucket_header)
    #@tools.write_json("s3_bucket",s3_bucket_header)
    def s3_bucket_size(self):
        s_no=1
        s3 = boto3.client('s3')
        s3_buckets = s3.list_buckets()['Buckets']
        for x in s3_buckets:
                
                bucket_name=x['Name'] 
                try:     
                        response_s3 = s3.list_objects(Bucket=bucket_name)['Contents']
                        size_list=[y['Size']for y in response_s3] 
                        total_size_bytes=reduce(lambda a,b : a+b,size_list)        
                        
                except :
                        pass
                if total_size_bytes >= 0 and total_size_bytes <= 1024:
                        bucket_size = str(total_size_bytes) +" B"
                elif total_size_bytes >= 1024 and total_size_bytes <= 1023999:
                        bucket_size = str(total_size_bytes/1000) +" KB"
                elif total_size_bytes >= 1024000 and total_size_bytes <= 999999999:
                        bucket_size = str(total_size_bytes/1e+6) +" MB"
                elif total_size_bytes >=1e+9 :
                        bucket_size = str(total_size_bytes/1e+9) +" GB"
                yield([s_no,bucket_name,bucket_size])
                s_no=s_no+1
    
    def iam_report(self):
        client=boto3.client('iam')
        response = client.get_credential_report()['Content']
        with open('./output_csv/iam_report.csv','w') as f:
                f.write(response.decode('utf-8'))

    def concat(self):
        path = './output_csv/'
        all_files = glob.glob(os.path.join(path, "*.csv"))
        writer = pd.ExcelWriter('AWS_inventory.xlsx', engine='xlsxwriter')
        for f in all_files:
            df = pd.read_csv(f)
            df.to_excel(writer, sheet_name=os.path.splitext(os.path.basename(f))[0], index=False)
        writer.save()




if __name__ == "__main__":
    i=inventory()
    with ThreadPoolExecutor(max_workers=5) as t:
        t1=t.submit(i.instance_inv())
        t2=t.submit(i.des_elb())
        t3=t.submit(i.des_elb_v2())
        t4=t.submit(i.network())
        t5=t.submit(i.gateway())
        
    with ThreadPoolExecutor(max_workers=5) as t1:
        t6=t1.submit(i.i_snapshot())            
        t7=t1.submit(i.iam_report)
        t8=t1.submit(i.elastic_cache())
        t9=t1.submit(i.rds_DB())        
        t10=t1.submit(i.eip())
        t11=t1.submit(i.rds_snapshot())
        t12=t1.submit(i.s3_bucket_size())
        #t12=t.submit(i.instance_audit())       
    i.concat() 
        
        

