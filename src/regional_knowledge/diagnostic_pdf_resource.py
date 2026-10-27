"""Fixed public synthetic transport probe; never corpus evidence or PDF export."""
import logging

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, ResourceLink

from .contracts import FetchOutput
from .read_authority import ReadAuthorityError

FIXTURE_ID = 'diagnostic:synthetic-pdf-resource-link:v1'
FIXTURE_URL = 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf'
LOG = logging.getLogger(__name__)


async def fetch_fixture(startup, principal):
    authority = startup.authority
    verified = None
    try:
        # The same actual bearer, destination and current public_search guard
        # as native fetch. Public synthetic bytes have no source/scan scope.
        verified = await authority.resolve_oauth(principal.access_token,
            destination=startup.backend.destination)
        await authority.require(verified, 'public_search')
        output = FetchOutput(id=FIXTURE_ID,title='Diagnostic synthetic PDF',
            text='Diagnostic synthetic PDF resource; not source evidence.',
            url=FIXTURE_URL,status='ok')
        result = CallToolResult(content=[ResourceLink(type='resource_link',
            uri=FIXTURE_URL,name='synthetic-diagnostic.pdf',mime_type='application/pdf',size=13264)],
            structured_content={'result':output.model_dump(mode='json')})
        await authority.recheck(verified)
        LOG.info('diagnostic_pdf_resource state=issued purpose=synthetic_transport')
        return result
    except ReadAuthorityError:
        LOG.info('diagnostic_pdf_resource state=denied purpose=synthetic_transport')
        raise ToolError('{"status":"denied","reason":"public_search_required"}') from None
    finally:
        if verified is not None:authority.release(verified)
