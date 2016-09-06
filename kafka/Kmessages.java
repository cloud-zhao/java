import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;


public class Kmessages{
	private static int max(int a,int b){
		if(a>b)
			return a;
		else 
			return b;
	}
	
	private static String bts(ByteBuffer bf){
		Charset charset=null;
		CharsetDecoder decode=null;
		
		try{
			charset=Charset.forName("UTF-8");
			decode=charset.newDecoder();
			return decode.decode(bf.asReadOnlyBuffer()).toString();
		}catch(Exception e){
			return "";
		}
	}

	public static void main(String[] args){
		if(args.length<1)
			System.exit(1);
		File file=new File(args[0]);
		FileChannel cl=null;
		try{
			cl=new RandomAccessFile(file,"rw").getChannel();
		}catch(Exception e){
			System.exit(1);
		}
		int start=0;
		ByteBuffer head=ByteBuffer.allocate(12);
		int res=0;
		while(true){
			try{
				res=cl.read(head,start);
			}catch(Exception e){}
			if(res==-1)
				break;
			head.rewind();
			start+=12;
			long offset=head.getLong();
			int size=head.getInt();
			if(size<14){
				System.out.println("Offset: "+offset+" Ms: NULL");
				head.clear();
				continue;
			}
			ByteBuffer ms=ByteBuffer.allocate(size);
			try{
				cl.read(ms,start);
			}catch(Exception e){}
			ms.rewind();
			start+=size;
			byte version=ms.get(4);
			int head_v=4+(version == 1 ? 10 : 2);
			int keysize=ms.getInt(head_v);
			int plso=head_v+4+max(0,keysize);
			int psize=ms.getInt(plso);
			if(size<0){
				System.out.println("Offset: "+offset+" Ms: NULL");
				head.clear();
				continue;
			}else{
				ByteBuffer bm=ms.duplicate();
				bm.position(plso+4);
				bm=bm.slice();
				bm.limit(psize);
				bm.rewind();
				System.out.println("Offset: "+offset+" Ms: "+bts(bm));
				head.clear();
			}
		}
		if(cl!=null)
			try{
				cl.close();
			}catch(Exception e){}
	}
}
