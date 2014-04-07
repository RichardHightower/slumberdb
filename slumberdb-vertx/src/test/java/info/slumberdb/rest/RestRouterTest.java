package info.slumberdb.rest;


import org.boon.primitive.Int;
import org.junit.Test;

import static org.boon.Sets.set;
import static org.boon.Str.equalsOrDie;


/**
 */
public class RestRouterTest {

    @Test
    public  void test() {
        RestRouter restRouter = new RestRouter("/foobarbar/",
                set("user/points/", "user/categories/"
                ));

        String dispatchId = restRouter.dispatchId("/foobarbar/user/points/");
        equalsOrDie("user/points/", dispatchId);
        Int.equalsOrDie(1, restRouter.routes());

        dispatchId = restRouter.dispatchId("/lovedove/user/points/");
        equalsOrDie("user/points/", dispatchId);
        Int.equalsOrDie(2, restRouter.routes());


        dispatchId = restRouter.dispatchId("/lovedove/user/points/");
        equalsOrDie("user/points/", dispatchId);
        Int.equalsOrDie(2, restRouter.routes());


        dispatchId = restRouter.dispatchId("/ass/monkey/user/points/");
        equalsOrDie("user/points/", dispatchId);
        Int.equalsOrDie(3, restRouter.routes());



    }
}
