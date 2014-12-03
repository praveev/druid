/*
 * Druid - a distributed column store.
 * Copyright 2014 Yahoo Inc.
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
package io.druid.common.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.druid.db.DefaultPasswordProvider;

/**
 * Implement this for different ways to (optionally securely) access db passwords.
 */
@JsonTypeInfo(use= JsonTypeInfo.Id.NAME, property="type", defaultImpl = DefaultPasswordProvider.class)
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="default", value=DefaultPasswordProvider.class),
})
public interface PasswordProvider 
{
  public String getPassword();
}
