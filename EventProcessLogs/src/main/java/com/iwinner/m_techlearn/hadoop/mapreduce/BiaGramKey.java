package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class BiaGramKey implements WritableComparable<BiaGramKey> {
	String word1;
	String word2;

	@Override
	public String toString() {
		return "{word1=" + word1 + ", word2=" + word2 + "}";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((word1 == null) ? 0 : word1.hashCode());
		result = prime * result + ((word2 == null) ? 0 : word2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BiaGramKey other = (BiaGramKey) obj;
		if (word1 == null) {
			if (other.word1 != null)
				return false;
		} else if (!word1.equals(other.word1))
			return false;
		if (word2 == null) {
			if (other.word2 != null)
				return false;
		} else if (!word2.equals(other.word2))
			return false;
		return true;
	}

	public String getWord1() {
		return word1;
	}

	public void setWord1(String word1) {
		this.word1 = word1;
	}

	public String getWord2() {
		return word2;
	}

	public void setWord2(String word2) {
		this.word2 = word2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		word1 = in.readUTF();
		word2 = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(word1);
		out.writeUTF(word2);
	}

	@Override
	public int compareTo(BiaGramKey key) {
		int diff = word1.compareTo(key.word1);
		if (diff == 0) {
			diff = word2.compareTo(key.word2);
		}
		return diff;
	}

}
