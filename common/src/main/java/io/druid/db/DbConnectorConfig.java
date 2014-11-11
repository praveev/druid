/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.db;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 */
public class DbConnectorConfig
{
  private PasswordProvider securePasswordProvider;
  private final AtomicBoolean passwordProviderInitialized = new AtomicBoolean(false);
  private static final Logger log = new Logger(DbConnectorConfig.class);
  
  @JsonProperty
  private boolean createTables = true;

  @JsonProperty
  @NotNull
  private String connectURI = null;

  @JsonProperty
  @NotNull
  private String user = null;

  @JsonProperty
  @NotNull
  private String password = null;

  @JsonProperty
  private String passwordKey = null;

  @JsonProperty
  private String passwordProvider = null;

  @JsonProperty
  private boolean useValidationQuery = false;

  @JsonProperty
  private String validationQuery = "SELECT 1";

  public DbConnectorConfig() { }

  public DbConnectorConfig(String passwordKey, String passwordProvider) {
    this.passwordKey = passwordKey;
    this.passwordProvider = passwordProvider;
  }

  public boolean isCreateTables()
  {
    return createTables;
  }

  public String getConnectURI()
  {
    return connectURI;
  }

  public String getUser()
  {
    return user;
  }

  public String getPassword() 
  {
    String finalPassword = password;
    if (passwordProvider != null && passwordKey != null) {
      if (!passwordProviderInitialized.getAndSet(true)) {
        try {
          securePasswordProvider = ((PasswordProvider)Class.forName(passwordProvider.trim()).newInstance());
          Map<String, String> config = Maps.<String, String>newHashMap();
          config.put("passwordKey", passwordKey);
          securePasswordProvider.init(config);
          finalPassword = securePasswordProvider.getPassword();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          log.error(String.format("Could not initialize PasswordProvider with class %s, exception caught: %s", passwordProvider, e));
        }
      }
    }
    return finalPassword;
  }

  public String getPasswordKey() 
  {
    return passwordKey;
  }

  public String getPasswordProvider() 
  {
    return passwordProvider;
  }

  public boolean isUseValidationQuery()
  {
    return useValidationQuery;
  }

  public String getValidationQuery() {
    return validationQuery;
  }

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + connectURI + '\'' +
           ", user='" + user + '\'' +
           ", password=****" +
           ", useValidationQuery=" + useValidationQuery +
           ", validationQuery='" + validationQuery + '\'' +
           '}';
  }
}
