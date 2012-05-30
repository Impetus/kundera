///*
// * Copyright 2011 Impetus Infotech.
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.impetus.kundera.tests.crossdatastore.pickr;
//
//import org.junit.runner.RunWith;
//import org.junit.runners.Suite;
//import org.junit.runners.Suite.SuiteClasses;
//
//
//
///**
// * Test suite for Pickr application
// * DDL for running test suite with in-built secondary indexes:
// * ----------------------------------------------------------------
// *  use Pickr;	
//	create column family PHOTOGRAPHER with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type and column_metadata=[{column_name: PHOTOGRAPHER_NAME, validation_class:UTF8Type, index_type: KEYS}];
//	create column family ALBUM with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type and column_metadata=[{column_name: ALBUM_NAME, validation_class:UTF8Type, index_type: KEYS},{column_name: ALBUM_DESC, validation_class:UTF8Type, index_type: KEYS}];
//	create column family PHOTO with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type and column_metadata=[{column_name: PHOTO_CAPTION, validation_class:UTF8Type, index_type: KEYS},{column_name: PHOTO_DESC, validation_class:UTF8Type, index_type: KEYS}];
//	create column family PHOTOGRAPHER_ALBUM with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
//	create column family ALBUM_PHOTO with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
// * 
// * DDL for running test suite with Lucene Indexing
// * ----------------------------------------------------------------
// 	use Pickr;
//	create column family PHOTOGRAPHER with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
//	create column family ALBUM with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
//	create column family PHOTO with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
//	create column family PHOTOGRAPHER_ALBUM with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
//	create column family ALBUM_PHOTO with comparator=UTF8Type and default_validation_class=UTF8Type and key_validation_class=UTF8Type;
// *
// * @author amresh.singh
// */
//@RunWith(Suite.class)
//@SuiteClasses({
//    PickrTestUni_1_1_1_1.class,
//    PickrTestUni_1_1_1_M.class,
//    PickrTestUni_1_M_1_M.class,
//    PickrTestUni_1_M_M_M.class,
//    PickrTestUni_M_1_1_M.class,
//    PickrTestUni_M_M_1_1.class,
//    PickrTestUni_M_M_M_M.class,    
//    /*PickrTestBi_1_1_1_1.class,
//    PickrTestBi_1_1_1_M.class,
//    PickrTestBi_1_M_1_M.class,
//    PickrTestBi_1_M_M_M.class,
//    PickrTestBi_M_1_1_M.class,
//    PickrTestBi_M_M_1_1.class,
//    PickrTestBi_M_M_M_M.class*/
//    
//})
//public class PickrTestSuite
//{    
//
//}
