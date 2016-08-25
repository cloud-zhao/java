import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.SimplifiedObjectMeta;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.BucketLoggingResult;
import com.aliyun.oss.model.BucketReferer;
import com.aliyun.oss.model.BucketWebsiteResult;
import com.aliyun.oss.model.SetBucketCORSRequest;
import com.aliyun.oss.model.SetBucketCORSRequest.CORSRule;
import com.aliyun.oss.model.SetBucketLifecycleRequest;
import com.aliyun.oss.model.SetBucketLoggingRequest;
import com.aliyun.oss.model.SetBucketWebsiteRequest;
import com.aliyun.oss.model.Bucket;

class FileDu extends Thread{
    private String dn;
    private OSSClient client;
    private String na=null;
    private final int mk=1000;
    private long dir_size=0;
    private int file_num=0;
    private ObjectListing o_list=null;
    private String bucketName="";

    public FileDu(String dir_name,OSSClient ossClient,String bn){
    	this.dn=dir_name;
        this.client=ossClient;
	this.bucketName=bn;
    }
    
    public void run(){
	do{
		ListObjectsRequest lor=new ListObjectsRequest(bucketName);
		o_list=client.listObjects(lor.withMarker(na).withMaxKeys(mk).withPrefix(dn));
		List<OSSObjectSummary> sums = o_list.getObjectSummaries();
		na = o_list.getNextMarker();
		for(OSSObjectSummary s : sums){
			dir_size+=s.getSize();
			file_num++;
		}
	}while(o_list.isTruncated());
	System.out.println(dn + " Size: "+(dir_size/1024)+" KB\tFile_num: "+file_num);
    }
}

public class AliyunDu{
	public static void main(String[] args){

		if(args.length<5){
			System.out.println("Usage: java AliyunDu endpoint"+
			" keyId keySecret bucketName prefix");
			System.exit(-1);
		}
        
	       	OSSClient client = new OSSClient(endpoint, accessKeyId, accessKeySecret);
		
		long start_total=System.currentTimeMillis();
	        String endpoint = args[0];
	        String accessKeyId = args[1];
	        String accessKeySecret = args[2];
		String BN = args[3];
		String mb=args[4]+"/";
		String dl="/";
		String n_marker=null;
		Vector<Thread> ths=new Vector<Thread>();
		List<String> all_dir=new ArrayList<String>();
		long file_all_size=0;
    		ObjectListing o_list=null;
		do{
			ListObjectsRequest list_or=new ListObjectsRequest("tvmbase");
			list_or=list_or.withMarker(n_marker);
			list_or=list_or.withMaxKeys(1000);
			list_or=list_or.withPrefix(mb);
			o_list=client.listObjects(list_or.withDelimiter(dl));
			List<OSSObjectSummary> sums = o_list.getObjectSummaries();
			all_dir.addAll(o_list.getCommonPrefixes());
			for(OSSObjectSummary s : sums){
				file_all_size+=s.getSize();
			}
			n_marker = o_list.getNextMarker();
			//System.out.println("next_marker: "+n_marker);
		}while(o_list.isTruncated());
		for(String s : all_dir){
			Thread mt=new FileDu(s,client,BN);
			ths.add(mt);
			mt.start();
		}
		for(Thread iT : ths){
			try{
				iT.join();
			}catch(InterruptedException e){};
		}
		long end_total=System.currentTimeMillis();
		if(file_all_size!=0){
			System.out.println(mb+" File Size: "+file_all_size);
		}
		System.out.println("Total time consuming:"+(end_total-start_total)+" ms");
	        client.shutdown();
	}
}
