package de.hhu.bsinfo.dxapp;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * "Hello world" example DXRAM application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class HelloApplication extends AbstractApplication {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "HelloApplication";
    }

    @Override
    public void main(final String[] p_args) {
        BootService bootService = getService(BootService.class);

        System.out.printf("\n");
        System.out.printf("  Hello! I am %s running on node %s.\n", getApplicationName(), NodeID.toHexStringShort(bootService.getNodeID()));
        System.out.printf("  My arguments are: %s\n", Arrays.toString(p_args));
        System.out.printf("\n");

        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
