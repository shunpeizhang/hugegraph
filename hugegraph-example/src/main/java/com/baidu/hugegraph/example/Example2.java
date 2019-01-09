/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.example;

import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;

public class Example2 {

    private static final Logger LOG = Log.logger(Example2.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("Example2 start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Example2.load(graph);
        traversal(graph);

        graph.close();

        HugeGraph.shutdown(30L);
    }

    public static void traversal(final HugeGraph graph) {
        GraphTraversalSource g = graph.traversal();

        // Query total vertices
//        System.out.println(g.V().toList());

        // Query by label, DONE
//        System.out.println(">>>> label = person vertices: " + g.V().hasLabel("person").toList());
//        System.out.println(">>>> label = knows edges: " + g.E().hasLabel("knows").toList());

        // Query by single secondary index
        System.out.println(">>>> name = marko vertices: " + g.V().has("name", "marko").toList());
    }

    public static void load(final HugeGraph graph) {
        SchemaManager schema = graph.schema();

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("weight").asDouble().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .useCustomizeStringId()
              .nullableKeys("age")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .useCustomizeStringId()
              .nullableKeys("price")
              .ifNotExist()
              .create();

        schema.indexLabel("personByNameAndCity")
              .onV("person")
              .by("name", "city")
              .secondary()
              .ifNotExist()
              .create();

        schema.indexLabel("softwareByNameAndLang")
              .onV("software")
              .by("name", "lang")
              .secondary()
              .ifNotExist()
              .create();
        
//        schema.indexLabel("personByAgeAndCity")
//              .onV("person")
//              .by("age", "city")
//              .secondary()
//              .ifNotExist()
//              .create();
//
//        schema.indexLabel("softwareByPrice")
//              .onV("software")
//              .by("price")
//              .range()
//              .ifNotExist()
//              .create();

        schema.edgeLabel("knows")
              .multiTimes()
              .sourceLabel("person")
              .targetLabel("person")
              .properties("date", "weight")
              .sortKeys("date")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("date", "weight")
              .nullableKeys("weight")
              .ifNotExist()
              .create();

//        schema.indexLabel("createdByDate")
//              .onE("created")
//              .by("date")
//              .secondary()
//              .ifNotExist()
//              .create();
//
//        schema.indexLabel("createdByWeight")
//              .onE("created")
//              .by("weight")
//              .range()
//              .ifNotExist()
//              .create();
//
//        schema.indexLabel("knowsByWeight")
//              .onE("knows")
//              .by("weight")
//              .range()
//              .ifNotExist()
//              .create();

        graph.tx().open();

        Vertex p_marko = graph.addVertex(T.label, "person", T.id, "p_marko", "name", "marko", "age", 29, "city", "Beijing");
        Vertex vadas = graph.addVertex(T.label, "person", T.id, "vadas", "name", "vadas", "age", 27, "city", "Beijing");
        Vertex josh = graph.addVertex(T.label, "person", T.id, "josh", "name", "josh", "age", 32, "city", "Beijing");
        Vertex peter = graph.addVertex(T.label, "person", T.id, "peter", "name", "peter", "age", 35, "city", "Shanghai");
        Vertex s_marko = graph.addVertex(T.label, "software", T.id, "s_marko", "name", "marko", "lang", "java", "price", 328);
        Vertex ripple = graph.addVertex(T.label, "software", T.id, "ripple", "name", "ripple", "lang", "java", "price", 199);

        p_marko.addEdge("knows", vadas, "date", "20160110", "weight", 0.5);
        p_marko.addEdge("knows", josh, "date", "20130220", "weight", 1.0);
        p_marko.addEdge("created", s_marko, "date", "20171210", "weight", 0.4);
        josh.addEdge("created", s_marko, "date", "20091111", "weight", 0.4);
        josh.addEdge("created", ripple, "date", "20171210", "weight", 1.0);
        peter.addEdge("created", s_marko, "date", "20170324", "weight", 0.2);

        graph.tx().commit();
    }
}
