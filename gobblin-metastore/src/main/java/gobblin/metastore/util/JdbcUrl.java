/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore.util;

import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;


public class JdbcUrl {
  private static final String PREFIX = "jdbc:";
  private final URIBuilder builder;

  private JdbcUrl() {
    builder = new URIBuilder();
  }

  private JdbcUrl(String url) throws MalformedURLException, URISyntaxException {
    if (!url.startsWith(PREFIX)) {
    throw new MalformedURLException();
    }
    builder = new URIBuilder(url.substring(PREFIX.length()));
  }

  public static JdbcUrl create() {
    return new JdbcUrl();
  }

  public static JdbcUrl parse(String url) throws MalformedURLException, URISyntaxException {
    return new JdbcUrl(url);
  }

  public JdbcUrl setScheme(String scheme) {
    builder.setScheme(scheme);
    return this;
  }

  public JdbcUrl setHost(String host) {
    builder.setHost(host);
    return this;
  }

  public JdbcUrl setPort(int port) {
    builder.setPort(port);
    return this;
  }

  public JdbcUrl setPath(String path) {
    builder.setPath("/" + path);
    return this;
  }

  public JdbcUrl setUser(String user) {
    return setParameter("user", user);
  }

  public JdbcUrl setPassword(String password) {
    return setParameter("password", password);
  }

  public JdbcUrl setParameter(String param, String value) {
    builder.setParameter(param, value);
    return this;
  }

  @Override
  public String toString() {
    try {
    return PREFIX + builder.build().toString();
    } catch (URISyntaxException e) {
    return null;
    }
  }
}
