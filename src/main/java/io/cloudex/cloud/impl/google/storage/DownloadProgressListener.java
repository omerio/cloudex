/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudex.cloud.impl.google.storage;

import io.cloudex.framework.cloud.api.Callback;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.common.base.Stopwatch;

/**
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public  class DownloadProgressListener implements MediaHttpDownloaderProgressListener {
	
	private final static Log log = LogFactory.getLog(DownloadProgressListener.class);
	
	private final Stopwatch stopwatch;
	
	private Callback callback;

	public DownloadProgressListener(Callback callback) {
		this.stopwatch = Stopwatch.createStarted();
		this.callback = callback;
	}

	@Override
	public void progressChanged(MediaHttpDownloader downloader) {
		switch (downloader.getDownloadState()) {
		case MEDIA_IN_PROGRESS:
			log.info("Progress: " + Double.toString(Math.round(downloader.getProgress() * 100.00)));
			break;
		case MEDIA_COMPLETE:
			if(stopwatch.isRunning()) {
				stopwatch.stop();
			}
			log.info(String.format("Download is complete! (%s)", stopwatch));
			if(this.callback != null) {
				this.callback.execute();
			}
			break;
		case NOT_STARTED:
			break;
		}
	}
	
	/**
	 * @param callback the callback to set
	 */
	public void setCallback(Callback callback) {
		this.callback = callback;
	}
}