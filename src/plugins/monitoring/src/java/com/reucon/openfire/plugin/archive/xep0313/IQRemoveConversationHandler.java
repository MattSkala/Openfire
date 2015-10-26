package com.reucon.openfire.plugin.archive.xep0313;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.IQ;
import org.xmpp.packet.JID;
import org.xmpp.packet.PacketExtension;
import org.xmpp.packet.PacketError;
import org.dom4j.Element;
import org.jivesoftware.openfire.auth.UnauthorizedException;
import org.jivesoftware.openfire.handler.IQHandler;
import org.jivesoftware.openfire.session.LocalClientSession;
import org.jivesoftware.database.DbConnectionManager;
import com.reucon.openfire.plugin.archive.xep.AbstractIQHandler;

/**
 * IQ Handler for removing conversations from archive.
 */
public class IQRemoveConversationHandler extends AbstractIQHandler {

  private static final Logger Log = LoggerFactory.getLogger(IQHandler.class);
  private static final String NAMESPACE = "kewe:archive";
  private static final String MODULE_NAME = "Archive Remove Conversation Query Handler";

  private static final String REMOVE_MESSAGES_FROM = "UPDATE ofMessageArchive SET removedFrom = 1 WHERE fromJID = ? AND toJID = ?";
  private static final String REMOVE_MESSAGES_TO = "UPDATE ofMessageArchive SET removedTo = 1 WHERE fromJID = ? AND toJID = ?";

  protected IQRemoveConversationHandler() {
    super(MODULE_NAME, "remove-conversation", NAMESPACE);
  }

  @Override
  public IQ handleIQ(IQ packet) throws UnauthorizedException {
    LocalClientSession session = (LocalClientSession) sessionManager.getSession(packet.getFrom());

    // If no session was found then answer with an error (if possible)
    if (session == null) {
      Log.error("Error during resource binding. Session not found in " +
          sessionManager.getPreAuthenticatedKeys() +
          " for key " +
          packet.getFrom());
      return buildErrorResponse(packet);
    }

    JID fromJid = packet.getFrom();
    Element element = packet.getChildElement();
    String participantJid = element.attributeValue("participant");

    removeConversation(fromJid.toBareJID(), participantJid);

    sendAcknowledgementResult(packet, session);

    return null;
  }

  private void removeConversation(String fromJid, String participantJid) {
    Connection con = null;
    PreparedStatement pstmt1 = null;
    PreparedStatement pstmt2 = null;
    try {
      con = DbConnectionManager.getConnection();
      pstmt1 = con.prepareStatement(REMOVE_MESSAGES_FROM);
      pstmt1.setString(0, fromJid);
      pstmt1.setString(1, participantJid);
      pstmt1.execute();
      pstmt2 = con.prepareStatement(REMOVE_MESSAGES_TO);
      pstmt2.setString(0, participantJid);
      pstmt2.setString(1, fromJid);
      pstmt2.execute();
    } catch (SQLException sqle) {
      Log.error("Error removing conversation", sqle);
    } finally {
      DbConnectionManager.closeConnection(pstmt1, con);
      DbConnectionManager.closeConnection(pstmt2, con);
    }
  }

  /**
   * Send result packet to client acknowledging query.
   * @param packet Received query packet
   * @param session Client session to respond to
   */
  private void sendAcknowledgementResult(IQ packet, LocalClientSession session) {
    IQ result = IQ.createResultIQ(packet);
    session.process(result);
  }

  /**
   * Create error response to send to client
   * @param packet
   * @return
   */
  private IQ buildErrorResponse(IQ packet) {
    IQ reply = IQ.createResultIQ(packet);
    reply.setChildElement(packet.getChildElement().createCopy());
    reply.setError(PacketError.Condition.internal_server_error);
    return reply;
  }
}
