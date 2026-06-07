package com.imap.bpm.infrastructure.security;

import com.imap.platform.security.AbstractServiceTokenProvider;
import org.springframework.stereotype.Component;

@Component
public class BpmServiceTokenProvider extends AbstractServiceTokenProvider {
    @Override
    protected String microName() { return "bpm"; }
}
