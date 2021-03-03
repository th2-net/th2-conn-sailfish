/******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.exactpro.th2.conn

import com.exactpro.sf.configuration.suri.SailfishURI
import com.exactpro.sf.externalapi.IServiceFactory
import com.exactpro.sf.externalapi.IServiceListener
import com.exactpro.sf.externalapi.IServiceProxy
import com.exactpro.sf.externalapi.ISettingsProxy
import com.exactpro.th2.conn.configuration.ConnectivityConfiguration
import com.exactpro.th2.common.schema.dictionary.DictionaryType.LEVEL1
import com.exactpro.th2.common.schema.dictionary.DictionaryType.MAIN
import com.exactpro.th2.common.schema.factory.CommonFactory
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito.`when`
import org.mockito.Mockito.any
import org.mockito.Mockito.eq
import org.mockito.Mockito.mock
import java.io.InputStream
import com.exactpro.sf.externalapi.DictionaryType as SailfishDictionaryType

class TestServiceLoading {
    @Test
    fun testServiceLoading() {
        val serviceFactory = mock(IServiceFactory::class.java)
        val listener = mock(IServiceListener::class.java)
        val proxy = mock(IServiceProxy::class.java)

        val settings = SettingsProxy(setOf(
            SailfishDictionaryType.MAIN,
            SailfishDictionaryType.LEVEL1
        ))

        `when`(serviceFactory.createService(any(), any(), any())).thenReturn(proxy)
        `when`(proxy.settings).thenReturn(settings)

        val commonFactory = mock(CommonFactory::class.java)

        val mainInputStream = mock(InputStream::class.java)
        val level1InputStream = mock(InputStream::class.java)

        val mainDictionaryUri = SailfishURI.parse(MAIN.name)
        val level1DictionaryUri = SailfishURI.parse(LEVEL1.name)

        `when`(commonFactory.readDictionary(eq(MAIN))).thenReturn(mainInputStream)
        `when`(commonFactory.readDictionary(eq(LEVEL1))).thenReturn(level1InputStream)

        `when`(serviceFactory.registerDictionary(
            eq(MAIN.name),
            eq(mainInputStream),
            eq(true)
        )).thenReturn(mainDictionaryUri)

        `when`(serviceFactory.registerDictionary(
            eq(LEVEL1.name),
            eq(level1InputStream),
            eq(true)
        )).thenReturn(level1DictionaryUri)

        val config = mock(ConnectivityConfiguration::class.java)

        `when`(config.settings).thenReturn(mapOf())

        MicroserviceMain.loadService(
            serviceFactory,
            commonFactory,
            config,
            listener
        )

        Assertions.assertEquals(settings.getDictionary(SailfishDictionaryType.MAIN), mainDictionaryUri)
        Assertions.assertEquals(settings.getDictionary(SailfishDictionaryType.LEVEL1), level1DictionaryUri)
    }

    private class SettingsProxy(private val dictionaryTypes: Set<SailfishDictionaryType>) : ISettingsProxy {
        private val dictionaries = hashMapOf<SailfishDictionaryType, SailfishURI>()

        override fun getParameterNames(): MutableSet<String> = TODO("Not yet implemented")
        override fun getParameterType(name: String?): Class<*> = TODO("Not yet implemented")
        override fun <T : Any?> getParameterValue(name: String?): T = TODO("Not yet implemented")
        override fun setParameterValue(name: String?, value: Any?): Unit = TODO("Not yet implemented")
        override fun getDictionaryTypes(): Set<SailfishDictionaryType> = dictionaryTypes
        override fun getDictionary(dictionaryType: SailfishDictionaryType): SailfishURI? = dictionaries[dictionaryType]
        override fun setDictionary(dictionaryType: SailfishDictionaryType, dictionaryUri: SailfishURI) {
            dictionaries[dictionaryType] = dictionaryUri
        }
    }
}