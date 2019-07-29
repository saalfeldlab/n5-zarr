/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.blosc;

import java.io.IOException;
import java.util.Arrays;

import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Writer;

/**
 * Lazy {@link BloscCompression} test using the abstract base class.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class N5FSTest extends AbstractN5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(testDirPath);
	}

	@Override
	protected Compression[] getCompressions() {

		return Arrays.stream(new JBlosc().listCompressors().split(",")).map(
				a -> new BloscCompression(a, 6, BloscCompression.SHUFFLE, 0, 1)).toArray(BloscCompression[]::new);
	}
}
