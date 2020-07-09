/*
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
 */
package com.exactpro.th2;

import static java.lang.System.out;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

public class RxJavaTest {

    /**
     * This test include the next operators: groupby, window, publish
     */
    @Test
    @Ignore
    public void test() {
        AtomicInteger count = new AtomicInteger(0);
        FlowableProcessor<Msg> processor = UnicastProcessor.create();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            out.println("start");
            Random ran = new Random();
            for(int i = 1; i <= 100; i++) {
                processor.onNext(new Msg()
                        .data("d" + i)
                        .seq(count.incrementAndGet())
                        .direction(i % 2 == 0));
                try {
                    Thread.sleep(ran.nextInt(100));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            processor.onComplete();
            out.println("end");
        });

        processor.doOnError(Throwable::printStackTrace)
                .groupBy(msg -> msg.direction)
                .map(entry -> {
                    Flowable<Batch> batchFlowable = entry.window(100, TimeUnit.MILLISECONDS, 3)
                            .flatMap(msgs -> msgs.toList().toFlowable())
                            .filter(list -> !list.isEmpty())
                            .map(list -> new Batch()
                                    .msgs(list))
                            .publish()
                            .autoConnect(2);

                    batchFlowable.map(Batch::toRaw)
                            .subscribe(i -> out.println('['  + Thread.currentThread().getName() + "] " + i));

                    batchFlowable.map(Batch::toParsed)
                            .subscribe(i -> out.println('['  + Thread.currentThread().getName() + "] " + i));

                    return  batchFlowable;
                }).doOnComplete(() -> {
                    out.println("Complite 1");
                })
                .subscribe();
        out.println("End of test");;
    }

    private static class Batch {
        int seq;
        boolean direction;
        List<Msg> msgs;

        public Batch msgs(List<Msg> msgs) {
            Msg firstMsg = msgs.get(0);
            this.seq = firstMsg.seq;
            this.direction = firstMsg.direction;
            this.msgs = msgs;
            return this;
        }

        public String toRaw() {
            return "Raw - " + this;
        }

        public String toParsed() {
            return "Parsed - " + this;
        }

        @Override
        public String toString() {
            return "Batch{" +
                    "seq=" + seq +
                    ", direction=" + direction +
                    ", msgs=" + msgs +
                    '}';
        }
    }

    private static class Msg {
        int seq;
        String data;
        boolean direction;

        public Msg seq(int seq) {
            this.seq = seq;
            return this;
        }

        public Msg data(String data) {
            this.data = data;
            return this;
        }

        public Msg direction(boolean direction) {
            this.direction = direction;
            return this;
        }

        @Override
        public String toString() {
            return "Msg{" +
                    "seq=" + seq +
                    ", data='" + data + '\'' +
                    '}';
        }
    }
}
