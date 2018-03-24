
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;

import java.util.List;

public class ViewHandler extends Thread {
    JChannel ch;
    MergeView view;

    public ViewHandler(JChannel ch, MergeView view) {
        this.ch = ch;
        this.view = view;
    }

    public void run() {
        List<View> subgroups = view.getSubgroups();
        View tmp_view = subgroups.get(0); // picks the first
        Address local_addr = ch.getAddress();
        if (!tmp_view.getMembers().contains(local_addr)) {
            System.out.println("Not member of the new primary partition ("
                    + tmp_view + "), will re-acquire the state");
            try {
                ch.getState(null, 30000);
            } catch (Exception ex) {
            }
        } else {
            System.out.println("Member of the new primary partition ("
                    + tmp_view + "), will do nothing");
        }
    }
}