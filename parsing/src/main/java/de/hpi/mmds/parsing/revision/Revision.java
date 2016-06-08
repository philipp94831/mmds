package de.hpi.mmds.parsing.revision;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Revision implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8344153876742311682L;
	private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
	private final long articleId;
	private long userId;
	private Date timestamp;
	private String username;
	private Integer textLength = 1;
	private boolean minor = false;

	public Revision(long articleId) {
		this.articleId = articleId;
	}

	public long getArticleId() {
		return articleId;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public String getTimestampAsString() {
		return DATE_FORMAT.format(timestamp);
	}

	public void setTimestamp(String timestamp) throws ParseException {
		this.timestamp = DATE_FORMAT.parse(timestamp);
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Integer getTextLength() {
		return textLength;
	}

	public void setTextLength(Integer textLength) {
		this.textLength = textLength;
	}

	public boolean isMinor() {
		return minor;
	}

	public void setMinor(boolean minor) {
		this.minor = minor;
	}

}
