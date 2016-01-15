package com.onestong.component.open.SeaWeedFSClient;

import java.util.ArrayList;

import com.onestong.component.open.SeaWeedFSClient.ServerLocations.ServerLocation;

import io.netty.handler.codec.http.HttpHeaders;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/*
 *  WeedFSClient Class
 */
public class WeedFSClient {

	// master address & port number
	private String masterAddress;
	private String masterPort;

	private WeedAssignedInfo assignedInfo = null;
	private ServerLocations locations = null;

	public WeedFSClient(String address, String port) {
		this.masterAddress = address;
		this.masterPort = port;
	}

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		vertx.fileSystem().readFile("/Users/xnq/Documents/tmp/7.jpg",file->{
			System.out.println(file.result().length());
			
			new WeedFSClient("182.254.223.219", "9333").upload(file.result(), handler->{
				if(handler.succeeded()){
					System.out.println(handler.result());
				}else{
					System.out.println(handler.cause());
				}
			});;
		});
	}
	
	public void upload(Buffer file, Handler<AsyncResult<Buffer>> handler) {
		if (file == null || file.length() == 0) {
			handler.handle(Future.failedFuture(new IllegalArgumentException("file cannot be empty")));
		}
		Vertx vert = Vertx.vertx();
		HttpClient client = vert.createHttpClient();
		// 1. send assign request and get fid
		client.requestAbs(HttpMethod.GET, "http://" + this.masterAddress + ":" + this.masterPort + "/dir/assign",
				response -> {
					if (response.statusCode() == 200) {
						response.handler(buffer -> {
							JsonObject json = new JsonObject(buffer.toString());
							assignedInfo = new WeedAssignedInfo();
							assignedInfo.setCount(json.getInteger("count"));
							assignedInfo.setFid(json.getString("fid"));
							assignedInfo.setPublicUrl(json.getString("publicUrl"));
							assignedInfo.setUrl(json.getString("url"));

							HttpClientRequest request = client.requestAbs(HttpMethod.PUT,
									"http://" + assignedInfo.getPublicUrl() + "/" + assignedInfo.getFid());
							request.handler(rs -> {
								rs.bodyHandler(bf -> {
									JsonObject tmp = new JsonObject(bf.toString());
									if(!tmp.containsKey("error")){
										handler.handle(Future.succeededFuture(Buffer.buffer(assignedInfo.getFid())));
									}else{
										handler.handle(Future.failedFuture(new IllegalArgumentException(bf.toString())));
									}
								});
							});
							request.putHeader(HttpHeaders.Names.CONTENT_TYPE, HttpHeaders.Values.MULTIPART_FORM_DATA
									+ "; " + HttpHeaders.Values.BOUNDARY + "=----------HV2ymHFg03ehbqgZCaKO6jyH");
							request.setChunked(true);
							request.write(bulidBody(file)).end();
						});
					} else {
						handler.handle(Future.failedFuture(new IllegalArgumentException("get weedfs fid failed ")));
					}
				}).end();
	}

	public static Buffer bulidBody(Buffer file) {
		// 头
		Buffer buffer = Buffer.buffer();
		String boundary = "----------HV2ymHFg03ehbqgZCaKO6jyH";
		// 传输内容
		StringBuffer contentBody = new StringBuffer("--" + boundary);
		String endBoundary = "\r\n--" + boundary + "--\r\n";

		buffer.appendString(contentBody.toString());
		// 2. 处理文件上传
		buffer.appendString("\r\n")

				.appendString("Content-Disposition:form-data; name=\"")

				.appendString("file" + "\"; ") // form中field的名称

				.appendString("filename=\"")

				.appendString("test.jpg" + "\"") // 上传文件的文件名，包括目录

				.appendString("\r\n")

				.appendString("Content-Type:application/octet-stream")

				.appendString("\r\n\r\n");
		// 开始真正向服务器写文件
		buffer.appendBuffer(file);
		buffer.appendString("------------HV2ymHFg03ehbqgZCaKO6jyH");
		buffer.appendString("------------HV2ymHFg03ehbqgZCaKO6jyH--\r\n");

		// 3. 写结尾
		buffer.appendString(endBoundary);
		return buffer;
	}
	/*
	 * example: fid = 3,01637037d6 write file to local file
	 */
	public void read(String fid, Handler<AsyncResult<Buffer>> handler) {
		if (fid == null || fid.length() == 0) {
			handler.handle(Future.failedFuture(new IllegalArgumentException("Fid cannot be empty")));
		}
		String volumnId = fid.split(",")[0];
		// 1. send quest to get volume address

		Vertx vertx = Vertx.vertx();

		HttpClient client = vertx.createHttpClient();

		client.requestAbs(HttpMethod.GET,
				"http://" + this.masterAddress + ":" + this.masterPort + "/" + "dir/lookup?volumeId=" + volumnId,
				resHandler -> {
					if (resHandler.statusCode() == 200) {
						resHandler.bodyHandler(by -> {
							// {"volumeId":"5","locations":[{"url":"127.0.0.1:8080","publicUrl":"127.0.0.1:8080"}]}
							JsonObject json = new JsonObject(by.toString());
							JsonArray _locations = json.getJsonArray("locations");
							locations = new ServerLocations();
							locations.locations = new ArrayList<ServerLocation>();
							for (int i = 0; i < _locations.size(); i++) {
								locations.locations
										.add( locations.new ServerLocation(_locations.getJsonObject(i).getString("publicUrl"),
												_locations.getJsonObject(i).getString("url")));
							}
							client.requestAbs(HttpMethod.GET, "http://" + locations.getOnePublicUrl() + "/" + fid,
									res -> {
										
										res.exceptionHandler(ex ->{
											
											handler.handle(Future.failedFuture(ex
													));

										});
									if(res.statusCode() == 200){
										res.bodyHandler(downFile ->{
											handler.handle(Future.succeededFuture(downFile));
										});
									}else{
										handler.handle(Future.failedFuture(
												new IllegalArgumentException("sever error code " + res.statusMessage())));
									}
							}).end();

						});
					} else {
						handler.handle(Future.failedFuture(
								new IllegalArgumentException("sever error code " + resHandler.statusMessage())));
					}
				}).end();

	}

	
	// /*
	// * delete the file
	// */
	//
	// public RequestResult delete(String fid) {
	//
	// if (fid == null || fid.length() == 0) {
	// throw new IllegalArgumentException("Fid cannot be empty");
	// }
	//
	// RequestResult result = new RequestResult();
	//
	// String volumnId = fid.split(",")[0];
	// ServerLocations locations = null;
	//
	// BufferedReader in = null;
	//
	// // 1. send quest to get volume address
	// try {
	// in = new BufferedReader(new InputStreamReader(
	// sendHttpGetRequest("http://" + this.masterAddress + ":" + this.masterPort
	// + "/",
	// "dir/lookup?volumeId=" + volumnId, "GET")));
	// String inputLine;
	// StringBuffer response = new StringBuffer();
	//
	// while ((inputLine = in.readLine()) != null) {
	// response.append(inputLine);
	// }
	// Gson gson = new Gson();
	// locations = gson.fromJson(response.toString(), ServerLocations.class);
	//
	// } catch (Exception e) {
	// throw new RuntimeException(e.toString());
	// } finally {
	// try {
	// in.close();
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
	//
	// // 2. delete the file
	// try {
	//
	// HttpURLConnection con = null;
	// URL requestUrl = new URL("http://" + locations.getOnePublicUrl() + "/" +
	// fid);
	// con = (HttpURLConnection) requestUrl.openConnection();
	//
	// con.setRequestMethod("DELETE");
	//
	// // add request header
	// con.setRequestProperty("User-Agent", "");
	// int responseCode = con.getResponseCode();
	//
	// if (responseCode == 200) {
	// result.setSuccess(true);
	// } else {
	// result.setSuccess(false);
	// }
	// } catch (Exception e) {
	// throw new RuntimeException(e.toString());
	// }
	// return result;
	// }
	//
	// /*
	// * Used to send request to WeedFS server
	// */
	// private InputStream sendHttpGetRequest(String host, String
	// requestUrlDetail, String method) throws Exception {
	//
	// HttpURLConnection con = null;
	// URL requestUrl = new URL(host.toString() + requestUrlDetail);
	// con = (HttpURLConnection) requestUrl.openConnection();
	//
	// // optional default is GET
	// con.setRequestMethod(method);
	//
	// // add request header
	// con.setRequestProperty("User-Agent", "");
	// int responseCode = con.getResponseCode();
	//
	// return con.getInputStream();
	// }
	//
	// public static void main(String[] args) {
	// RequestResult result = null;
	// WeedFSClient client = new WeedFSClient("localhost", "9333");
	// try {
	// result = client.write("/WeedFS/test.data");
	// client.read(result.getFid(), "/WeedFS/test.data1");
	// client.delete(result.getFid());
	// File file = new File("/WeedFS/test.data1");
	// file.delete();
	// client.read(result.getFid(), "/WeedFS/test.data1");
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// System.out.println(result.toString());
	// }
}
