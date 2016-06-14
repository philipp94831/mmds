package de.hpi.mmds.parsing.revision;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Revision implements Serializable {

	private static final long serialVersionUID = 8344153876742311682L;
	private final long articleId;
	// not static because of serialization and synchronization issues
	private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
	private boolean minor = false;
	private Integer textLength = 1;
	private Date timestamp;
	private long userId;

	public Revision(long articleId) {
		this.articleId = articleId;
	}

	public long getArticleId() {
		return articleId;
	}

	public Integer getTextLength() {
		return textLength;
	}

	public void setTextLength(Integer textLength) {
		this.textLength = textLength;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) throws ParseException {
		this.timestamp = DATE_FORMAT.parse(timestamp);
	}

	public String getTimestampAsString() {
		return DATE_FORMAT.format(timestamp);
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public boolean isMinor() {
		return minor;
	}

	public void setMinor(boolean minor) {
		this.minor = minor;
	}

}
