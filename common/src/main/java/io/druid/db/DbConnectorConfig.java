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
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;

import io.druid.common.config.PasswordProvider;
import io.druid.common.utils.PropUtils;

import javax.validation.constraints.NotNull;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class DbConnectorConfig
{
  private static final Logger log = new Logger(DbConnectorConfig.class);
  private final AtomicBoolean passwordProviderInitialized = new AtomicBoolean(false);
  
  @JsonProperty
  private boolean createTables = true;

  @JsonProperty
  @NotNull
  private String connectURI = null;

  @JsonProperty
  @NotNull
  private String user = null;

  @JsonProperty
  private String password = null;

  @JsonProperty
  private String passwordProviderConfig = null;

  @JsonProperty
  private String passwordProvider = null;

  @JsonProperty
  private boolean useValidationQuery = false;

  @JsonProperty
  private String validationQuery = "SELECT 1";

  public DbConnectorConfig() { }

  public DbConnectorConfig(String passwordProvider, String passwordProviderConfig, String password) {
    this.passwordProvider = passwordProvider;
    this.passwordProviderConfig = passwordProviderConfig;
    this.password = password;
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
    if(password == null && passwordProvider == null) {
      throw new IAE("Db config error.Both password and passwordProvider can't be null");
    }

    if (passwordProvider != null && !passwordProviderInitialized.get()) {
      synchronized(this) {
        if(!passwordProviderInitialized.get()) {
          try {
            log.info("Initializing password provider %s with config %s", passwordProvider, passwordProviderConfig);
            
            PasswordProvider pwdProvider = (PasswordProvider)Class.forName(passwordProvider.trim()).newInstance();
            if(passwordProviderConfig == null) {
              pwdProvider.init(Collections.<String,String>emptyMap());
            } else {
              pwdProvider.init(PropUtils.parseStringAsMap(passwordProviderConfig, ";", ":"));
            }
            password = pwdProvider.getPassword();
            passwordProviderInitialized.set(true);
            
            log.info("password provider initialized successfully.");
          } catch(ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException("Failed to get password from PasswordProvider", e);
          }
        }
      }
    }
    return password;
  }

  public String getPasswordProviderConfig() 
  {
    return passwordProviderConfig;
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
