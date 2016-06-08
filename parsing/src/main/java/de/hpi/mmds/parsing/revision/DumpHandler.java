package de.hpi.mmds.parsing.revision;

import com.github.philipp94831.stax2parser.DefaultHandler;

import javax.xml.namespace.QName;
import java.io.IOException;
import java.text.ParseException;
import java.util.Stack;

public class DumpHandler extends DefaultHandler {

	private final DumpWriter out;
	private StringBuilder buf;
	private long currentArticle;
	private int currentNamespace;
	private Revision currentRevision;
	private boolean err;
	private boolean inText = false;
	private Stack<String> parents;

	public DumpHandler(DumpWriter out) {
		this.out = out;
	}

	@Override
	public void characters(String ch) {
		if (inText) {
			buf.append(ch);
		}
	}

	@Override
	public void endElement(QName qname) {
		if (isInId()) {
			currentArticle = Long.parseLong(buf.toString());
		}
		if (isInNamespace()) {
			currentNamespace = Integer.parseInt(buf.toString());
		}
		if (isInContributorId()) {
			currentRevision.setUserId(Long.parseLong(buf.toString()));
		}
		if (isInTimestamp()) {
			try {
				currentRevision.setTimestamp(buf.toString());
			} catch (ParseException e) {
				err = true;
			}
		}
		if (isInRevision()) {
			if (!err && currentRevision.getUserId() != 0 && currentNamespace == 0) {
				out.write(currentRevision);
			}
			currentRevision = null;
		}
		parents.pop();
	}

	private boolean isInId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("page");
	}

	private boolean isInNamespace() {
		return parents.peek().equals("ns") && parents.elementAt(parents.size() - 2).equals("page");
	}

	private boolean isInContributorId() {
		return parents.peek().equals("id") && parents.elementAt(parents.size() - 2).equals("contributor");
	}

	private boolean isInTimestamp() {
		return parents.peek().equals("timestamp") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	private boolean isInRevision() {
		return parents.peek().equals("revision");
	}

	@Override
	public void startDocument() {
		parents = new Stack<>();
	}

	@Override
	public void startElement(QName qname) {
		parents.push(qname.getLocalPart());
		if (isInId() || isInContributorId() || isInTimestamp() || isInNamespace()) {
			inText = true;
			buf = new StringBuilder();
		}
		if (isInRevision()) {
			currentRevision = new Revision(currentArticle);
			err = false;
		}
		if (isInMinor()) {
			currentRevision.setMinor(true);
		}
	}

	@Override
	public void attribute(QName qName, String value) {
		if (isInText() && qName.getLocalPart().equals("bytes")) {
			currentRevision.setTextLength(Integer.parseInt(value));
		}
	}

	private boolean isInText() {
		return parents.peek().equals("text") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	private boolean isInMinor() {
		return parents.peek().equals("minor") && parents.elementAt(parents.size() - 2).equals("revision");
	}

	public void close() throws IOException {
		out.close();
	}
}
