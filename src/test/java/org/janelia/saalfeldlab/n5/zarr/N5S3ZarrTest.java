/**
 * Copyright (c) 2019, Stephan Saalfeld
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.zarr;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedIntType;
import net.imglib2.type.numeric.integer.UnsignedLongType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Bzip2Compression;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class N5S3ZarrTest {

	static private String endpoint = "https://s3.embassy.ebi.ac.uk/";
	static private String bucket = "idr";
	static private String object = "zarr/v0.1/6001240.zarr";

	/*
	@Test
	public void test6001240Local() throws Exception {

		N5ZarrReader n5 = new N5ZarrReader("/opt/data/6001240.zarr");

		assertTrue(n5.exists("0"));
		Map<String, Class<?>> attributesMap = n5.listAttributes("/");
		Assert.assertNotNull(attributesMap.get("multiscales"));

		DatasetAttributes da = n5.getDatasetAttributes("/0");
		DataBlock<?> db = n5.readBlock("/0", da, 0, 0, 0, 0, 0);
		int[] size = db.getSize();
		short[] data = (short[]) db.getData();
		System.out.println(data);
	}
	 */

	@Test
	public void test6001240() throws Exception {

		AwsClientBuilder.EndpointConfiguration cfg = new AwsClientBuilder.EndpointConfiguration(endpoint, "");
		AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
		builder.setEndpointConfiguration(cfg);
		builder.withPathStyleAccessEnabled(true);
		AmazonS3 s3 = builder.build();
		N5S3ZarrReader n5 = new N5S3ZarrReader(s3, bucket, object);

		assertTrue(n5.exists("0"));
		Map<String, Class<?>> attributesMap = n5.listAttributes("/");
		Assert.assertNotNull(attributesMap.get("multiscales"));

		// Load a well known block
		DatasetAttributes da = n5.getDatasetAttributes("/0");
		DataBlock<?> db = n5.readBlock("/0", da, 0, 0, 0, 0, 0);
		assertArrayEquals(new int[]{271, 275, 1, 1, 1}, db.getSize());
		short[] data = (short[]) db.getData();
		assertEquals(8, data[0]);
		assertEquals(9, data[1]);
		assertEquals(8, data[2]);
		assertEquals(10, data[3]);

		// Attempt to load a missing block. Should be null.
		assertNull(n5.readBlock("/0", da, 1000, 1000, 1000, 1000, 1000));
	}

}
