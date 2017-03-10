package org.cassandra.sde.file;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

public class CassandraFileManager {

	private Cluster cluster = null;

	public CassandraFileManager() {
		cluster = Cluster.builder().addContactPoint("localhost").build();
	}

	public void insert(String file_path) {
		Session session = cluster.connect();
		session.execute("use usa;");
		File source_file = new File(file_path);
		String filename = source_file.getName();
		String format = filename.substring(filename.indexOf(".") + 1, filename.length());
		UUID id = UUID.randomUUID();
		Date date = new Date();
		try {
			FileInputStream fis = new FileInputStream(source_file);
			ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
			byte[] b = new byte[1024 * 1024];
			int n;
			int count = 0;
			BatchStatement bs = new BatchStatement();
			List<byte[]> list = new ArrayList<>();
			while ((n = fis.read(b)) != -1) {
				byte[] temp = new byte[n];
				System.arraycopy(b, 0, temp, 0, n);
				list.add(temp);
				System.out.println(temp.length);
				count++;
			}
			for (int i = 0; i < list.size(); i++) {
				
				session.execute(new SimpleStatement(
						"INSERT INTO FILE_TABLE (id,file_part_no,file_name,format,part_amount,date,data) VALUES (?,?,?,?,?,?,?)",
						id, i, filename, format, list.size(), date, ByteBuffer.wrap(list.get(i))));
			}


		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void getFile(UUID id){
		
		Session session = cluster.connect();
		session.execute("use usa;");
		SimpleStatement statement=new SimpleStatement("SELECT * FROM FILE_TABLE WHERE id=?",id);
		ResultSet rs=session.execute(statement);
		List<byte[]> list = new ArrayList<>(12);
		for(Row row:rs){
			ByteBuffer buffer=row.getBytes("data");
			byte[] bytes=buffer.array();
			int number=row.getInt("file_part_no");
			list.add(number,bytes);
		}
		try {
			generateFile(list);
		} catch (Exception e) {
			e.printStackTrace();
		}
		session.close();
		cluster.close();
	}
	
	public void toFile(byte[] bytes, String filePath, String fileName) {
		BufferedOutputStream bos = null;
		FileOutputStream fos = null;
		File file = null;
		try {
			File dir = new File(filePath);
			if (!dir.exists()) {// 判断文件目录是否存在
				dir.mkdirs();
			}
			file = new File(filePath + "\\" + fileName);
			fos = new FileOutputStream(file);
			bos = new BufferedOutputStream(fos);
			bos.write(bytes);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bos != null) {
				try {
					bos.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	

	public void generateFile(List<byte[]> list) throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
		byte[] buffer = null;
		for (int i = 0; i < list.size(); i++) {
			byte[] b = list.get(i);
			bos.write(b,0,b.length);
		}
		buffer = bos.toByteArray();
		toFile(buffer, "C:\\Users\\Phil\\Desktop", "merge.pptx");
	}
	
	public static void main(String[] args) {
		//new CassandraFileManager().insert("E:\\我的文档\\电子书\\Beginning Apache Cassandra development.pdf");
		//new CassandraFileManager().insert("C:\\Users\\Public\\Videos\\Sample Videos\\Wildlife.wmv");
		//new CassandraFileManager().insert("E:/时空数据引擎方案汇报-2016-12-20.pptx");
		new CassandraFileManager().getFile(UUID.fromString("a26b3af0-2d1f-4cce-8636-a34563c3b878"));
	}

}
