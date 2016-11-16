package com.anthonykim.datamgr;

import org.junit.Test;

/**
 * Created by hyuk0 on 2016-11-03.
 */
public class RaceConditionTest {

    private class SharedObject {
        public synchronized void useSynch(String name) {
            System.err.println(name + " use before");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println(name + " use after");
            notifyAll();
        }
    }

    private class SimpleThread extends Thread {
        private SharedObject object;

        public SimpleThread(SharedObject object) {
            this.object = object;
        }

        @Override
        public void run() {
            object.useSynch(this.getName());
        }
    }

    //@Test
    public void testRaceCondition() {
        SharedObject object = new SharedObject();

        SimpleThread t1 = new SimpleThread(object);
        SimpleThread t2 = new SimpleThread(object);
        SimpleThread t3 = new SimpleThread(object);
        SimpleThread t4 = new SimpleThread(object);
        SimpleThread t5 = new SimpleThread(object);

        t1.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        try {
            Thread.sleep(12000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}